"""
Provide parallel control flow primitives.
"""
module Control

using Base.Threads
using Distributed
using ..Launched
using ..Resources

export SimdFlag
export default_simd
export default_batch_factor
export s_foreach
export s_collect
export t_foreach
export t_collect
export d_foreach
export d_collect

"""
The `@simd` directive to use for an inner loop.
"""
const SimdFlag = Union{Bool,Symbol,Val{true},Val{false},Val{:ivdep}}

"""
The default `@simd` directive to apply to the inner loops.

This is `:ivdep` because the code here assumes all steps are entirely independent. Any coordination
is expected to be done using the appropriate `ParallelResources`.
"""
const default_simd = :ivdep

"""
The default number of batches to run in each thread.

Scheduling is done in equal-size batches where on average each thread will execute `batch_factor`
such batches. Setting this to a value large than one compensates for variability between computation
time of such batches. However setting this to higher values also increases scheduling overhead. The
default is `4` which is assumed to be a reasonable compromise.
"""
const default_batch_factor = 4

"""
    s_foreach(step::Function, resources::ParallelResources, values::collection;
              simd=default_simd)

Perform `step` for each `value` in `values`, serially, using the current thread in the current
process.

This is implemented as a simple loop using the specified `simd`, which repeatedly invoked
`step(resources, value)`. The return value of `step` is discarded.

Having this makes it easier to convert a parallel loop to a serial one, for example for measuring
performance.
"""
function s_foreach(
    step::Function,
    resources::ParallelResources,
    values;
    simd::SimdFlag = default_simd,
)::Nothing
    if simd == :ivdep
        @simd ivdep for value in values
            step(resources, value)
        end

    elseif simd == true
        @simd for value in values
            step(resources, value)  # Only seems unreached
        end

    elseif simd == false
        for value in values  # Foo
            step(resources, value)  # Only seems unreached
        end

    else
        throw(ArgumentError("invalid simd flag: $(simd)"))
    end

    return nothing
end

"""
    s_collect(collect::Function, step::Function, resources::ParallelResources, values::collection;
              simd=default_simd)

Perform `step` for each `value` in `values`, serially, using the current thread in the current
process; `collect` the returned results into a single accumulator and return it.

This is implemented as a simple loop using the specified `simd`, which repeatedly invokes
`step(resources, value)`. The `collect(accumulator, step_result)` is likewise repeatedly invoked to
accumulate the step results into a single per-thread resource named `accumulator`. The return value
of `collect` is ignored.

!!! note

    The accumulator must be mutable. This is different from the classical `reduce` operation which
    takes two values and returns a merged result (possibly in different object). For example, to
    perform a sum of integers using `collect`, define the accumulator to be an array with a single
    entry.

Having this makes it easier to convert a parallel collection to a serial one, for example for
measuring performance.
"""
function s_collect(
    collect::Function,
    step::Function,
    resources::ParallelResources,
    values;
    simd::SimdFlag = default_simd,
)::Any
    accumulator = get_per_thread(resources, "accumulator")

    s_foreach(resources, values, simd = simd) do resources, value
        collect(accumulator, step(resources, value))
    end

    return accumulator
end

function batch_values_view(values, batch_size::Number, batch_index::Int)::Any
    return batches_values_view(values, batch_size, batch_index, batch_index)
end

function batches_values_view(
    values,
    batch_size::Number,
    first_batch_index::Int,
    last_batch_index::Int,
)::Any
    first_step_index = round(Int, (first_batch_index - 1) * batch_size) + 1
    last_step_index = round(Int, last_batch_index * batch_size)
    @assert 1 <= first_step_index &&
            first_step_index <= last_step_index && last_step_index <= length(values)
    return @views values[first_step_index:last_step_index]
end

function batches_data(
    step::Function,
    resources::ParallelResources,
    values,
    batch_factor::Int,
    minimal_batch::Int,
    simd::SimdFlag,
    runners_count::Int,
)::Tuple{Int,Number}
    @assert batch_factor > 0
    @assert minimal_batch > 0
    @assert runners_count > 1

    if length(values) <= minimal_batch
        return 1, length(values)
    end

    batches_count = runners_count * batch_factor
    batch_size = length(values) / batches_count
    if batch_size < minimal_batch
        batch_size = minimal_batch
        batches_count = round(Int, length(values) / minimal_batch)
    end

    return batches_count, batch_size
end

"""
    t_foreach(step::Function, resources::ParallelResources, values::collection;
              batch_factor=default_batch_factor, simd=default_simd)

Perform a step for each value in the collection, in parallel, using multiple threads in the current
process.

Scheduling is done in equal-size batches where on average each thread will execute `batch_factor`
such batches. Setting this to a value large than one compensates for variability between computation
time of such batches. However setting this to higher values also increases scheduling overhead.

Each batch will contain at least `minimal_batch` iterations, even if this means using a smaller
number of threads.

Each batch is executed as an inner loop using `s_foreach` with the specified `simd`.
"""
function t_foreach(
    step::Function,
    resources::ParallelResources,
    values;
    batch_factor::Int = default_batch_factor,
    minimal_batch::Int = 1,
    simd::SimdFlag = default_simd,
)::Nothing
    batches_count, batch_size =
        batches_data(step, resources, values, batch_factor, minimal_batch, simd, nthreads())

    if batches_count <= 1
        s_foreach(step, resources, values, simd = simd)
    elseif batches_count <= nthreads()
        t_foreach_up_to_nthreads(step, resources, values, batch_size, batches_count, simd)
    else
        t_foreach_more_than_threads(
            step,
            resources,
            values,
            batch_size,
            batches_count,
            simd,
        )
    end

    return nothing
end

function t_foreach_up_to_nthreads(
    step::Function,
    resources::ParallelResources,
    values,
    batch_size::Number,
    batches_count::Int,
    simd::SimdFlag = default_simd,
)::Nothing
    @threads for batch_index = 1:batches_count
        s_foreach(
            step,
            resources,
            batch_values_view(values, batch_size, batch_index),
            simd = simd,
        )
    end
    return nothing
end

function t_foreach_more_than_threads(
    step::Function,
    resources::ParallelResources,
    values,
    batch_size::Number,
    batches_count::Int,
    simd::SimdFlag = default_simd,
)::Nothing
    next_batch_index = Atomic{Int}(nthreads() + 1)
    @threads for batch_index = 1:nthreads()
        while batch_index <= batches_count
            s_foreach(
                step,
                resources,
                batch_values_view(values, batch_size, batch_index),
                simd = simd,
            )
            batch_index = atomic_add!(next_batch_index, 1)
        end
    end
    return nothing
end

function make_status_of_threads()::Array{Int,1}
    status_of_threads = Array{Symbol,1}(undef, nthreads())
    fill!(status_of_threads, :empty)
    return status_of_threads
end

"""
    t_collect(collect::Function, step::Function, resources::ParallelResources, values::collection;
              batch_factor=default_batch_factor, simd=default_simd)

Perform `step` for each `value` in `values`, serially, using multiple threads in the current
process; `collect` the returned results into a single accumulator and return it. To manage this,
a per-process `_pending_threads` resource is added.

This builds on `s_collect` to accumulate results in each thread. The per-thread accumulators are
merged together (in multiple threads) by invoking `collect(into_accumulator, from_accumulator)`.
The final result is returned.
"""
function t_collect(
    collect::Function,
    step::Function,
    resources::ParallelResources,
    values;
    batch_factor::Int = default_batch_factor,
    minimal_batch::Int = 1,
    simd::SimdFlag = default_simd,
)::Any
    batches_count, batch_size =
        batches_data(step, resources, values, batch_factor, minimal_batch, simd, nthreads())

    if batches_count <= 1
        return s_collect(collect, step, resources, values, simd = simd)
    end

    add_per_process!(resources, "_pending_threads", make = () -> Array{Int,1}(undef, 0))

    if batches_count <= nthreads()
        t_collect_up_to_nthreads(
            collect,
            step,
            resources,
            values,
            batch_size,
            batches_count,
            simd,
        )
    else
        t_collect_more_than_threads(
            collect,
            step,
            resources,
            values,
            batch_size,
            batches_count,
            simd,
        )
    end

    pending_threads = get_per_process(resources, "_pending_threads")
    @assert length(pending_threads) == 1
    @assert pending_threads[1] == 1
    return get_per_thread(resources, "accumulator", 1)
end

function t_collect_up_to_nthreads(
    collect::Function,
    step::Function,
    resources::ParallelResources,
    values,
    batch_size::Number,
    batches_count::Int,
    simd::SimdFlag,
)::Nothing
    @threads for batch_index = 1:batches_count
        s_collect(
            collect,
            step,
            resources,
            batch_values_view(values, batch_size, batch_index),
            simd = simd,
        )
        merge_accumulators(collect, resources)
    end

    return nothing
end

function t_collect_more_than_threads(
    collect::Function,
    step::Function,
    resources::ParallelResources,
    values,
    batch_size::Number,
    batches_count::Int,
    simd::SimdFlag,
)::Nothing
    next_batch_index = Atomic{Int}(nthreads() + 1)
    @threads for batch_index = 1:nthreads()
        while batch_index <= batches_count
            s_collect(
                collect,
                step,
                resources,
                batch_values_view(values, batch_size, batch_index),
                simd = simd,
            )
            batch_index = atomic_add!(next_batch_index, 1)
        end
        merge_accumulators(collect, resources)
    end

    return nothing
end

function merge_accumulators(collect::Function, resources::ParallelResources)::Nothing
    thread_id = threadid()
    while true
        other_id = 0
        with_per_process(resources, "_pending_threads") do pending_threads
            if length(pending_threads) > 0
                other_id = pop!(pending_threads)
            else
                push!(pending_threads, thread_id)
            end
        end
        other_id > 0 || break
        into_id = min(thread_id, other_id)
        from_id = max(thread_id, other_id)
        collect(
            get_per_thread(resources, "accumulator", into_id),
            get_per_thread(resources, "accumulator", from_id),
        )
        thread_id = into_id
    end

    return nothing
end

next_process = Atomic{Int}(myid())

function next_process!()::Int
    return 1 + mod(atomic_add!(next_process, 1), nprocs())
end

"""
    d_foreach(step::Function, resources::ParallelResources, values::collection;
              batch_factor=default_batch_factor, simd=default_simd)

Perform a step for each value in the collection in parallel using a single thread in each of the
processes (including the current one).

Scheduling is done in equal-size batches where on average each process will execute `batch_factor`
such batches. Setting this to a value large than one compensates for variability between computation
time of such batches. However setting this to higher values also increases scheduling overhead.

Each batch will contain at least `minimal_batch` iterations, even if this means using a smaller
number of processes.

Each batch is executed as an inner loop using `s_foreach` with the specified `simd`.
"""
function d_foreach(
    step::Function,
    resources::ParallelResources,
    values;
    batch_factor::Int = default_batch_factor,
    minimal_batch::Int = 1,
    simd::SimdFlag = default_simd,
)::Nothing
    batches_count, batch_size =
        batches_data(step, resources, values, batch_factor, minimal_batch, simd, nprocs())

    if batches_count <= 1
        s_foreach(step, resources, values, simd = simd)
    elseif batches_count <= nprocs()
        d_foreach_up_to_nprocs(step, resources, values, batch_size, batches_count, simd)
    else
        d_foreach_more_than_nprocs(step, resources, values, batch_size, batches_count, simd)
    end

    return nothing
end

function d_foreach_up_to_nprocs(
    step::Function,
    resources::ParallelResources,
    values,
    batch_size::Number,
    batches_count::Int,
    simd::SimdFlag,
)::Nothing
    @sync begin
        batch_index = 1
        while batch_index < batches_count
            process_id = next_process!()
            process_id != myid() || continue
            @spawnat process_id s_foreach(
                step,
                resources,
                batch_values_view(values, batch_size, batch_index),
                simd = simd,
            )
            batch_index += 1
        end

        s_foreach(
            step,
            resources,
            batch_values_view(values, batch_size, batch_index),
            simd = simd,
        )
    end

    return nothing
end

function d_foreach_more_than_nprocs(
    step::Function,
    resources::ParallelResources,
    values,
    batch_size::Number,
    batches_count::Int,
    simd::SimdFlag,
)::Nothing
    jobs_channel = RemoteChannel(() -> Channel{Any}(batches_count * 2))

    @sync begin
        for batch_index = 1:batches_count
            put!(jobs_channel, batch_values_view(values, batch_size, batch_index))
        end

        for _batch_index = 1:batches_count
            put!(jobs_channel, nothing)
        end

        for process_id in workers()
            @spawnat process_id run_jobs_batches(jobs_channel, step, resources, simd)
        end

        run_jobs_batches(jobs_channel, step, resources, simd)
    end

    return nothing
end

function run_jobs_batches(
    jobs_channel::RemoteChannel{Channel{Any}},
    step::Function,
    resources::ParallelResources,
    simd::SimdFlag,
)::Nothing
    while true
        batch_values = take!(jobs_channel)
        batch_values != nothing || break
        s_foreach(step, resources, batch_values, simd = simd)
    end

    return nothing
end

end # module
