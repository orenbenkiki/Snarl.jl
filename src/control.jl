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
export t_foreach
export d_foreach

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

Perform a step for each value in the collection, serially, using the current thread in the current
process.

This is implemented as a simple loop using the specified `simd`.

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

function batch_values(values, batch_index::Int, batch_size::Number)::Any
    first_step_index = round(Int, (batch_index - 1) * batch_size) + 1
    last_step_index = round(Int, batch_index * batch_size)
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
        s_foreach(step, resources, values, simd = simd)
        return 0, 0.0
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

    if batches_count <= 0
        return nothing
    end

    if batches_count <= nthreads()
        @threads for batch_index = 1:batches_count
            s_foreach(
                step,
                resources,
                batch_values(values, batch_index, batch_size),
                simd = simd,
            )
        end
    else
        next_batch_index = Atomic{Int}(nthreads() + 1)
        @threads for batch_index = 1:nthreads()
            while batch_index <= batches_count
                s_foreach(
                    step,
                    resources,
                    batch_values(values, batch_index, batch_size),
                    simd = simd,
                )
                batch_index = atomic_add!(next_batch_index, 1)
            end
        end
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

    if batches_count <= 0
        return nothing
    end

    if batches_count <= nprocs()
        @sync for batch_index = 1:batches_count
            @spawnat next_process!() s_foreach(
                step,
                resources,
                batch_values(values, batch_index, batch_size),
                simd = simd,
            )
        end
    else
        jobs_channel = RemoteChannel(() -> Channel{Tuple{Int,Any}}(batches_count * 2))
        started_channel = RemoteChannel(() -> Channel{Bool}(nworkers()))

        @sync begin
            for batch_index = 1:batches_count
                put!(
                    jobs_channel,
                    (batch_index, batch_values(values, batch_index, batch_size)),
                )
            end

            for batch_index = 1:batches_count
                put!(jobs_channel, (0, nothing))
            end

            function run_batches()::Nothing
                put!(started_channel, true)
                while true
                    batch_index, batch_values = take!(jobs_channel)
                    batch_index > 0 || break
                    s_foreach(step, resources, batch_values, simd = simd)
                    sleep(0.01)
                end
                return nothing
            end

            for process_index in workers()
                @spawnat process_index run_batches()
            end

            for _process_index in workers()
                take!(started_channel)
            end

            run_batches()
        end
    end

    return nothing
end

end # module
