"""
Provide parallel control flow primitives.
"""
module Control

using Base.Threads
using Distributed

using ..Launched
using ..Storage

export SimdFlag
export default_batch_factor
export default_minimal_batch_size
export default_prefer
export default_simd

export s_foreach
export s_collect
export t_foreach
export t_collect
export d_foreach
export d_collect
export dt_foreach
export dt_collect

"""
The `@simd` directive to use for an inner loop.
"""
const SimdFlag = Union{Bool,Symbol,Val{true},Val{false},Val{:ivdep}}

"""
The default `@simd` directive to apply to the inner loops.

This is `:ivdep` because the code here assumes all steps are entirely independent. Any
coordination is expected to be done using the appropriate `ParallelStorage`.
"""
const default_simd = :ivdep

"""
The default minimal number of iterations in a batch.

"""
const default_minimal_batch_size = 1

"""
The default number of batches to run in each thread.

Scheduling is done in equal-size batches where on average each thread will execute
`batch_factor` such batches. Setting this to a value large than one compensates for variability
between computation time of such batches. However setting this to higher values also increases
scheduling overhead. The default is `4` which is assumed to be a reasonable compromise.
"""
const default_batch_factor = 4

"""
    s_foreach(step::Function, storage::ParallelStorage, values::collection;
              simd::SimdFlag=default_simd)

Perform `step` for each `value` in `values`, serially, using the current thread in the current
process.

This is implemented as a simple loop using the specified `simd`, which repeatedly invoked
`step(storage, value)`. The return value of `step` is discarded.

Having this makes it easier to convert a parallel loop to a serial one, for example for comparing
parallel and serial performance.
"""
function s_foreach(
    step::Function,
    storage::ParallelStorage,
    values;
    simd::SimdFlag = default_simd,
)::Nothing
    if simd == :ivdep
        @simd ivdep for value in values
            step(storage, value)
        end

    elseif simd == true
        @simd for value in values
            step(storage, value)
        end

    elseif simd == false
        for value in values
            step(storage, value)
        end

    else
        throw(ArgumentError("Invalid simd flag: $(simd)"))
    end

    yield()  # Allow the scheduler to deal with contention every batch of operations.

    return nothing
end

"""
    s_collect(collect::Function, step::Function, storage::ParallelStorage, values::collection;
              into::AbstractString="accumulator", simd::SimdFlag=default_simd)

Perform `step` for each `value` in `values`, serially, using the current thread in the current
process; `collect` the returned results into a single accumulator and return it.

This is implemented as a simple loop using the specified `simd`, which repeatedly invokes
`step(storage, value)`. The `collect(accumulator, step_result)` is likewise repeatedly invoked to
accumulate the step results into a single per-thread value named (by default, `accumulator`). The return value
of `collect` is ignored.

!!! note

    The accumulator must be mutable. This is different from the classical `reduce` operation which
    takes two values and returns a merged result (possibly in different object). For example, to
    perform a sum of integers using `collect`, define the accumulator to be an array with a single
    entry.

Having this makes it easier to convert a parallel collection to a serial one, for example for
comparing parallel and serial performance.
"""
function s_collect(
    collect::Function,
    step::Function,
    storage::ParallelStorage,
    values;
    into::AbstractString = "accumulator",
    simd::SimdFlag = default_simd,
)::Any
    accumulator = get_per_thread(storage, into)

    s_foreach(storage, values, simd = simd) do storage, value
        collect(accumulator, step(storage, value))
    end

    return accumulator
end

function batch_values_view(values, batch_size::Number, batch_index::Int)::Any
    first_step_index = round(Int, (batch_index - 1) * batch_size) + 1
    last_step_index = round(Int, batch_index * batch_size)
    @assert 1 <= first_step_index &&
            first_step_index <= last_step_index &&
            last_step_index <= length(values)
    return @views @inbounds values[first_step_index:last_step_index]
end

function batch_values_views(
    values,
    batch_size::Number,
    first_batch_index::Int,
    batches_count::Int,
)::Array{Any,1}
    views = Array{Any,1}(undef, batches_count)
    for index = 1:batches_count
        views[index] = batch_values_view(values, batch_size, first_batch_index + index - 1)
    end
    return views
end

function batches_configuration(
    step::Function,
    storage::ParallelStorage,
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
        batches_count = floor(Int, length(values) / minimal_batch)
        batch_size = length(values) / batches_count
    end

    return batches_count, batch_size
end

"""
    t_foreach(step::Function, storage::ParallelStorage, values::collection;
              batch_factor::Int=default_batch_factor, simd::SimdFlag=default_simd)

Perform a step for each value in the collection, in parallel, using multiple threads in the current
process.

Scheduling is done in equal-size batches containing at least `minimal_batch` iterations in each,
where on average each thread will execute at most `batch_factor` such batches.

Each batch is executed as an inner loop using `s_foreach` with the specified `simd`.
"""
function t_foreach(
    step::Function,
    storage::ParallelStorage,
    values;
    batch_factor::Int = default_batch_factor,
    minimal_batch::Int = 1,
    simd::SimdFlag = default_simd,
)::Nothing
    batches_count, batch_size = batches_configuration(
        step,
        storage,
        values,
        batch_factor,
        minimal_batch,
        simd,
        nthreads(),
    )

    if batches_count <= 1
        s_foreach(step, storage, values, simd = simd)
    elseif batches_count <= nthreads()
        t_foreach_up_to_nthreads(step, storage, values, batch_size, batches_count, simd)
    else
        t_foreach_more_than_nthreads(step, storage, values, batch_size, batches_count, simd)
    end

    return nothing
end

function t_foreach_up_to_nthreads(
    step::Function,
    storage::ParallelStorage,
    values,
    batch_size::Number,
    batches_count::Int,
    simd::SimdFlag = default_simd,
)::Nothing
    @assert batches_count <= nthreads()

    @sync @threads for batch_index = 1:batches_count
        s_foreach(
            step,
            storage,
            batch_values_view(values, batch_size, batch_index),
            simd = simd,
        )
    end

    return nothing
end

function t_foreach_more_than_nthreads(
    step::Function,
    storage::ParallelStorage,
    values,
    batch_size::Number,
    batches_count::Int,
    simd::SimdFlag = default_simd,
)::Nothing
    @assert batches_count > nthreads()

    next_batch_index = Atomic{Int}(nthreads() + 1)
    @sync @threads for batch_index = 1:nthreads()
        while batch_index <= batches_count
            s_foreach(
                step,
                storage,
                batch_values_view(values, batch_size, batch_index),
                simd = simd,
            )
            batch_index = atomic_add!(next_batch_index, 1)
        end
    end

    return nothing
end

"""
    t_collect(collect::Function, step::Function, storage::ParallelStorage, values::collection;
              into::AbstractString="accumulator",
              batch_factor::Int=default_batch_factor,
              simd::SimdFlag=default_simd)

Perform `step` for each `value` in `values`, serially, using multiple threads in the current
process; `collect` the returned results into a single accumulator and return it. To manage this, a
per-process `_pending_threads` value is added containing a stack (array) of pending thread
identifiers (whose results were not collected yet).

This builds on `s_collect` to accumulate results in each thread. The per-thread accumulators are
merged together (using multiple threads) by invoking `collect(into_accumulator, from_accumulator)`.
The final result is returned.
"""
function t_collect(
    collect::Function,
    step::Function,
    storage::ParallelStorage,
    values;
    into::AbstractString = "accumulator",
    batch_factor::Int = default_batch_factor,
    minimal_batch::Int = 1,
    simd::SimdFlag = default_simd,
)::Any
    batches_count, batch_size = batches_configuration(
        step,
        storage,
        values,
        batch_factor,
        minimal_batch,
        simd,
        nthreads(),
    )

    if batches_count <= 1
        return s_collect(collect, step, storage, values, into = into, simd = simd)
    end

    add_per_process!(storage, "_pending_threads", make = () -> Array{Int,1}(undef, 0))

    if batches_count <= nthreads()
        t_collect_up_to_nthreads(
            collect,
            step,
            storage,
            values,
            batch_size,
            batches_count,
            into,
            simd,
        )
    else
        t_collect_more_than_threads(
            collect,
            step,
            storage,
            values,
            batch_size,
            batches_count,
            into,
            simd,
        )
    end

    pending_threads = get_per_process(storage, "_pending_threads")
    @assert length(pending_threads) == 1
    @assert @inbounds pending_threads[1] == 1
    return get_per_thread(storage, into, 1)
end

function t_collect_up_to_nthreads(
    collect::Function,
    step::Function,
    storage::ParallelStorage,
    values,
    batch_size::Number,
    batches_count::Int,
    into::AbstractString,
    simd::SimdFlag,
)::Nothing
    @assert batches_count <= nthreads()

    @sync @threads for batch_index = 1:batches_count
        s_collect(
            collect,
            step,
            storage,
            batch_values_view(values, batch_size, batch_index),
            into = into,
            simd = simd,
        )
        collect_thread_accumulator(collect, storage, into)
    end

    return nothing
end

function t_collect_more_than_threads(
    collect::Function,
    step::Function,
    storage::ParallelStorage,
    values,
    batch_size::Number,
    batches_count::Int,
    into::AbstractString,
    simd::SimdFlag,
)::Nothing
    @assert batches_count > nthreads()

    next_batch_index = Atomic{Int}(nthreads() + 1)
    @sync @threads for batch_index = 1:nthreads()
        while batch_index <= batches_count
            s_collect(
                collect,
                step,
                storage,
                batch_values_view(values, batch_size, batch_index),
                into = into,
                simd = simd,
            )
            batch_index = atomic_add!(next_batch_index, 1)
        end
        collect_thread_accumulator(collect, storage, into)
    end

    return nothing
end

function collect_thread_accumulator(
    collect::Function,
    storage::ParallelStorage,
    into::AbstractString,
)::Nothing
    thread_id = threadid()
    while true
        other_id = 0
        with_per_process(storage, "_pending_threads") do pending_threads
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
            get_per_thread(storage, into, into_id),
            get_per_thread(storage, into, from_id),
        )
        thread_id = into_id
    end

    return nothing
end

next_worker_id = Atomic{Int}(myid())

function next_worker!()::Int
    worker_id = myid()
    while worker_id == myid()
        worker_id = 1 + mod(atomic_add!(next_worker_id, 1), nprocs())
    end
    return worker_id
end

"""
    d_foreach(step::Function, storage::ParallelStorage, values::collection;
              batch_factor::Int=default_batch_factor, simd::SimdFlag=default_simd)

Perform a step for each value in the collection in parallel using a single thread in each of the
processes (including the current one).

Scheduling is done in equal-size batches containing at least `minimal_batch` iterations in each,
where on average each process (thread) will execute at most `batch_factor` such batches.

Each batch is executed as an inner loop using `s_foreach` with the specified `simd`.
"""
function d_foreach(
    step::Function,
    storage::ParallelStorage,
    values;
    batch_factor::Int = default_batch_factor,
    minimal_batch::Int = 1,
    simd::SimdFlag = default_simd,
)::Nothing
    batches_count, batch_size = batches_configuration(
        step,
        storage,
        values,
        batch_factor,
        minimal_batch,
        simd,
        nprocs(),
    )

    if batches_count <= 1
        s_foreach(step, storage, values, simd = simd)
    elseif batches_count <= nprocs()
        d_foreach_up_to_nprocs(step, storage, values, batch_size, batches_count, simd)
    else
        d_foreach_more_than_nprocs(step, storage, values, batch_size, batches_count, simd)
    end

    return nothing
end

function d_foreach_up_to_nprocs(
    step::Function,
    storage::ParallelStorage,
    values,
    batch_size::Number,
    batches_count::Int,
    simd::SimdFlag,
)::Nothing
    @assert batches_count <= nprocs()

    @sync begin
        for batch_index = 2:batches_count
            @spawnat next_worker!() s_foreach(
                step,
                storage,
                batch_values_view(values, batch_size, batch_index),
                simd = simd,
            )
        end

        s_foreach(step, storage, batch_values_view(values, batch_size, 1), simd = simd)
    end

    return nothing
end

function d_foreach_more_than_nprocs(
    step::Function,
    storage::ParallelStorage,
    values,
    batch_size::Number,
    batches_count::Int,
    simd::SimdFlag,
)::Nothing
    @assert batches_count > nprocs()

    jobs_channel = RemoteChannel(() -> Channel{Any}(batches_count + nprocs()))

    @sync begin
        next_batch_index = 1
        for worker_id in workers()
            @spawnat worker_id s_run_batches_from_jobs_channel(
                jobs_channel,
                step,
                storage,
                batch_values_view(values, batch_size, next_batch_index),
                simd,
            )
            next_batch_index += 1
        end

        send_jobs_batches(
            jobs_channel,
            values,
            batch_size,
            next_batch_index + 1,
            batches_count,
        )
        send_jobs_terminations(jobs_channel, nprocs())

        s_run_batches_from_jobs_channel(
            jobs_channel,
            step,
            storage,
            batch_values_view(values, batch_size, next_batch_index),
            simd,
        )
    end

    return nothing
end

function send_jobs_batches(
    jobs_channel::RemoteChannel{Channel{Any}},
    values,
    batch_size::Number,
    first_batch_index::Int,
    last_batch_index::Int,
)::Nothing
    for batch_index = first_batch_index:last_batch_index
        put!(jobs_channel, batch_values_view(values, batch_size, batch_index))
    end
end

function send_jobs_terminations(
    jobs_channel::RemoteChannel{Channel{Any}},
    terminations_count::Int,
)::Nothing
    for _ = 1:terminations_count
        put!(jobs_channel, nothing)
    end
    return nothing
end

function s_run_batches_from_jobs_channel(
    jobs_channel::RemoteChannel{Channel{Any}},
    step::Function,
    storage::ParallelStorage,
    batch_values,
    simd::SimdFlag,
)::Nothing
    while batch_values != nothing
        s_foreach(step, storage, batch_values, simd = simd)
        batch_values = take!(jobs_channel)
    end

    return nothing
end

"""
The policy to use to distribute work across processes.
"""
const PreferFlag = Union{Symbol,Val{:threads},Val{:distributed}}

"""
The default `prefer` policy to use to distribute work across processes.

This is `:threads` assuming that when the number of batches is low, it is better to run them all on
the current process to minimize memory usage and network traffic.
"""
const default_prefer = :threads

function verify_prefer(prefer::PreferFlag)::Nothing
    if prefer != :threads && prefer != :distributed
        throw(ArgumentError("Invalid prefer flag: $(prefer)"))
    end
    return nothing
end

"""
    dt_foreach(step::Function, storage::ParallelStorage, values::collection;
               batch_factor::Int=default_batch_factor, simd::SimdFlag=default_simd)

Perform a step for each value in the collection in parallel using multiple thread in multiple
processes (including the current one).

Scheduling is done in equal-size batches containing at least `minimal_batch` iterations in each,
where on average each thread will execute at most `batch_factor` such batches.

If fewer than the total number of threads are needed, then if `prefer` is `threads`, the minimal
number of processes will be used (maximizing the use of the threads in each). If `prefer` is
`distributed`, then the maximal number of processes will be used (minimizing the use of the threads
in each).

Each batch is executed as an inner loop using `s_foreach` with the specified `simd`.
"""
function dt_foreach(
    step::Function,
    storage::ParallelStorage,
    values;
    batch_factor::Int = default_batch_factor,
    minimal_batch::Int = 1,
    prefer::PreferFlag = default_prefer,
    simd::SimdFlag = default_simd,
)::Nothing
    verify_prefer(prefer)

    batches_count, batch_size = batches_configuration(
        step,
        storage,
        values,
        batch_factor,
        minimal_batch,
        simd,
        total_threads_count,
    )

    if batches_count <= 1
        s_foreach(step, storage, values, simd = simd)
    elseif prefer == :distributed || batches_count > total_threads_count
        dt_foreach_prefer_distributed(
            step,
            storage,
            values,
            batch_size,
            batches_count,
            simd,
        )
    elseif batches_count <= nthreads()
        t_foreach_up_to_nthreads(step, storage, values, batch_size, batches_count, simd)
    else
        dt_foreach_prefer_threads(step, storage, values, batch_size, batches_count, simd)
    end

    return nothing
end

function dt_foreach_prefer_distributed(
    step::Function,
    storage::ParallelStorage,
    values,
    batch_size::Number,
    batches_count::Int,
    simd::SimdFlag,
)::Nothing
    used_threads_count = min(batches_count, total_threads_count)
    jobs_channel = RemoteChannel(() -> Channel{Any}(batches_count + used_threads_count))
    used_threads_of_processes = compute_used_threads_of_processes(used_threads_count)

    next_batch_index = 1
    @sync begin
        for process = 1:nprocs()
            process != myid() || continue
            @inbounds used_threads_of_process = used_threads_of_processes[process]
            used_threads_of_process > 0 || continue
            @spawnat process t_run_batches_from_jobs_channel(
                jobs_channel,
                used_threads_of_process,
                step,
                storage,
                batch_values_views(
                    values,
                    batch_size,
                    next_batch_index,
                    used_threads_of_process,
                ),
                simd,
            )
            next_batch_index += used_threads_of_process
        end

        used_threads_of_process = @inbounds used_threads_of_processes[myid()]
        @assert used_threads_of_process > 0
        @threads for index = 1:used_threads_of_process
            if index == used_threads_of_process
                send_jobs_batches(
                    jobs_channel,
                    values,
                    batch_size,
                    next_batch_index + used_threads_of_process,
                    batches_count,
                )
                send_jobs_terminations(jobs_channel, used_threads_count)
            end
            s_run_batches_from_jobs_channel(
                jobs_channel,
                step,
                storage,
                batch_values_view(values, batch_size, next_batch_index + index - 1),
                simd,
            )
        end
    end

    return nothing
end

# TODO: This could be made more efficient if needed.
function compute_used_threads_of_processes(used_threads_count::Int)::Array{Int,1}
    used_threads_of_processes = zeros(Int, nprocs())

    remaining_threads_count = used_threads_count
    @assert remaining_threads_count > 0

    while true
        for offset = 1:nprocs()
            process = if (offset == 1)
                myid()
            else
                next_worker!()
            end

            if @inbounds used_threads_of_processes[process] <
                         threads_count_of_processes[process]
                @inbounds used_threads_of_processes[process] += 1
                remaining_threads_count -= 1
                if remaining_threads_count == 0
                    return used_threads_of_processes
                end
            end
        end
    end
end

function dt_foreach_prefer_threads(
    step::Function,
    storage::ParallelStorage,
    values,
    batch_size::Number,
    batches_count::Int,
    simd::SimdFlag,
)::Nothing
    used_processes = zeros(Bool, nprocs())
    used_processes[myid()] = true

    remaining_batches_count = batches_count - nthreads()
    @assert remaining_batches_count > 0

    @sync begin
        next_batch_index = 1
        while true
            worker_id = next_worker!()
            used_processes[worker_id] && continue
            used_processes[worker_id] = true

            threads_count = @inbounds min(
                remaining_batches_count,
                threads_count_of_processes[worker_id],
            )
            threads_count > 0 || continue

            @spawnat worker_id t_run_batches(
                step,
                storage,
                batch_values_views(values, batch_size, next_batch_index, threads_count),
                simd,
            )
            next_batch_index += threads_count

            remaining_batches_count -= threads_count
            remaining_batches_count > 0 || break
        end

        t_run_batches(
            step,
            storage,
            batch_values_views(values, batch_size, next_batch_index, nthreads()),
            simd,
        )
        next_batch_index += nthreads()

        @assert next_batch_index == batches_count + 1
    end
end

function t_run_batches(
    step::Function,
    storage::ParallelStorage,
    batch_values::Any,
    simd::SimdFlag,
)::Nothing
    @sync @threads for index = 1:length(batch_values)
        s_foreach(step, storage, batch_values[index], simd = simd)
    end

    return nothing
end

function t_run_batches_from_jobs_channel(
    jobs_channel::Distributed.RemoteChannel{Channel{Any}},
    threads_count::Int,
    step::Function,
    storage::ParallelStorage,
    batch_values::Array{Any,1},
    simd::SimdFlag,
)::Nothing
    @assert length(batch_values) == threads_count
    @sync @threads for index = 1:threads_count
        s_run_batches_from_jobs_channel(
            jobs_channel,
            step,
            storage,
            batch_values[index],
            simd,
        )
    end

    return nothing
end

end # module
