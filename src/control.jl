"""
Provide parallel control flow primitives.
"""
module Control

using Base.Threads
using Distributed

using ..Launched
using ..Storage

export SimdFlag, PreferFlag
export next_worker!
export default_batch_factor,
    default_minimal_batch_size, default_maximize_distribution, default_simd
export s_foreach, t_foreach, d_foreach, dt_foreach
export s_collect, t_collect, d_collect, dt_collect

# Data types:

"""
The `@simd` directive to use for an inner loop.
"""
const SimdFlag = Union{Bool,Symbol,Val{true},Val{false},Val{:ivdep}}

"""
The policy to use to distribute multi-threaded work across processes.

Specifying `maximize_processes` uses all the processes (servers), by using fewer threads in each
one.

Specifying `minimize_processes` uses the fewest processes (servers), by using all the threads in
each one.
"""
const DistributionPolicy = Union{Symbol,Val{:maximize_processes},Val{:minimize_processes}}

function verify_distribution(distribution::DistributionPolicy)::Nothing
    if distribution != :maximize_processes && distribution != :minimize_processes
        throw(ArgumentError("Invalid distribution policy: $(distribution)"))
    end
    return nothing
end

# Default parameter values:

"""
The default `@simd` directive to apply to the inner loops.

The default is `:ivdep` because the code here assumes all steps are entirely independent. Any
coordination is expected to be done using the appropriate `ParallelStorage`.
"""
const default_simd = :ivdep

"""
The default minimal number of iterations executed serially in a batch.

The default is `1`, not because it is the most useful, but because no other specific value is
generally useful either.
"""
const default_minimal_batch_size = 1

"""
The default number of batches to run in each thread.

Scheduling is done in equal-size batches where on average each thread will execute `batch_factor`
such batches. Setting this to a value large than one compensates for variability between computation
time of such batches. However setting this to higher values also increases scheduling overhead.

The default is `4` which is assumed to be a reasonable compromise.
"""
const default_batch_factor = 4

"""
The default `DistributionPolicy` policy to use to distribute multi-threaded work across processes.

Using `maximize_processes` uses all the processes (servers), by using fewer threads in each one.
Using `minimize_processes` uses the fewest processes (servers), by using all the threads in each
one.

The default is `maximize_processes` under the assumption that running few threads on each process
(that is, server) will gain from more per-server resources such as memory and I/O bandwidth. It is
also more likely that less threads will run as weaker hyper-threads.
"""
const default_distribution = :maximize_processes

# Workers:

next_worker_id = Atomic{Int}(myid())

"""
Return the next worker process to use.

This tries to spread the work evenly between all workers. Mainly intended to be used in `@spawnat
next_worker!() ...` for launching a single job on some other process (never on the current process).
"""
function next_worker!()::Int
    worker_id = myid()
    while worker_id == myid()
        worker_id = 1 + mod(atomic_add!(next_worker_id, 1), nprocs())
    end
    return worker_id
end

function next_workers!(workers_count::Int)::Array{Int,1}
    @assert 1 <= workers_count && workers_count <= nworkers()
    if workers_count == nworkers()
        return 2:nprocs()
    end

    worker_ids = Array{Int,1}(undef, workers_count)
    is_process_used = zeros(Bool, nprocs())
    remaining_workers_count = workers_count

    while remaining_workers_count > 0
        worker_id = next_worker!()
        if !is_process_used[worker_id]
            is_process_used[worker_id] = true
            worker_ids[remaining_workers_count] = worker_id
            remaining_workers_count -= 1
        end
    end

    return worker_ids
end

# Foreach:

"""
    s_foreach(step::Function, storage::ParallelStorage, values::collection;
              simd::SimdFlag=default_simd)

Perform `step` for each `value` in `values`, serially, using the current thread in the current
process.

This is implemented as a simple loop using the specified `simd`, which repeatedly invoked
`step(storage, value)`. The return value of `step` is discarded.

Having `s_foreach` makes it easier to convert a parallel loop to a serial one, for example for
comparing parallel and serial performance.
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

"""
    d_foreach(step::Function, storage::ParallelStorage, values::collection;
              batch_factor::Int=default_batch_factor, simd::SimdFlag=default_simd)

Perform a step for each value in the collection, in parallel, using a single thread in each of the
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

"""
    dt_foreach(step::Function, storage::ParallelStorage, values::collection;
               batch_factor::Int=default_batch_factor, simd::SimdFlag=default_simd)

Perform a step for each value in the collection, in parallel, using multiple threads in multiple
processes (including the current one).

Scheduling is done in equal-size batches containing at least `minimal_batch` iterations in each,
where on average each thread will execute at most `batch_factor` such batches.

If `distribution` is `maximize_processes`, use all the processes (servers), by using fewer threads
in each one. If it is `minimize_processes`, use the fewest processes (servers), by using all the
threads in each one.

Each batch is executed as an inner loop using `s_foreach` with the specified `simd`.
"""
function dt_foreach(
    step::Function,
    storage::ParallelStorage,
    values;
    batch_factor::Int = default_batch_factor,
    minimal_batch::Int = 1,
    distribution::DistributionPolicy = default_distribution,
    simd::SimdFlag = default_simd,
)::Nothing
    verify_distribution(distribution)

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
    elseif distribution == :maximize_processes
        dt_foreach_maximize_processes(
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
        dt_foreach_minimize_processes(
            step,
            storage,
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

function d_foreach_up_to_nprocs(
    step::Function,
    storage::ParallelStorage,
    values,
    batch_size::Number,
    batches_count::Int,
    simd::SimdFlag,
)::Nothing
    @assert 1 < batches_count && batches_count <= nprocs()

    @sync begin
        worker_ids = next_workers!(batches_count - 1)
        for batch_index = 1:(batches_count-1)
            @spawnat worker_ids[batch_index] s_foreach(
                step,
                storage,
                batch_values_view(values, batch_size, batch_index),
                simd = simd,
            )
        end

        s_foreach(
            step,
            storage,
            batch_values_view(values, batch_size, batches_count),
            simd = simd,
        )
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

    batches_channel = RemoteChannel(() -> Channel{Any}(batches_count))

    @sync begin
        next_batch_index = 1
        for worker_id in workers()
            @spawnat worker_id s_run_from_batches_channel(
                batches_channel,
                step,
                storage,
                batch_values_view(values, batch_size, next_batch_index),
                simd,
            )
            next_batch_index += 1
        end

        send_batches(
            batches_channel,
            values,
            batch_size,
            next_batch_index + 1,
            batches_count,
        )
        send_terminations(batches_channel, nprocs())
        close(batches_channel)

        s_run_from_batches_channel(
            batches_channel,
            step,
            storage,
            batch_values_view(values, batch_size, next_batch_index),
            simd,
        )
    end

    return nothing
end

function dt_foreach_maximize_processes(
    step::Function,
    storage::ParallelStorage,
    values,
    batch_size::Number,
    batches_count::Int,
    simd::SimdFlag,
)::Nothing
    used_threads_count = min(batches_count, total_threads_count)
    batches_channel = RemoteChannel(() -> Channel{Any}(batches_count))
    used_threads_of_processes = compute_used_threads_of_processes(used_threads_count)

    next_batch_index = 1
    @sync begin
        for process = 1:nprocs()
            process != myid() || continue
            @inbounds used_threads_of_process = used_threads_of_processes[process]
            used_threads_of_process > 0 || continue
            @spawnat process t_run_from_batches_channel(
                batches_channel,
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
                send_batches(
                    batches_channel,
                    values,
                    batch_size,
                    next_batch_index + index,
                    batches_count,
                )
                send_terminations(batches_channel, used_threads_count)
                close(batches_channel)
            end
            s_run_from_batches_channel(
                batches_channel,
                step,
                storage,
                batch_values_view(values, batch_size, next_batch_index + index - 1),
                simd,
            )
        end
    end

    return nothing
end

function dt_foreach_minimize_processes(
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

# Collect:

"""
    s_collect(collect::Function, step::Function, storage::ParallelStorage,
              values::collection; simd::SimdFlag=default_simd)

Perform `step` for each `value` in `values`, serially, using the current thread in the current
process; `collect` the returned results into per-current-thread accumulator(s) in the `storage`. The
final results should be fetched from the `storage`.

This is implemented as a simple loop using the specified `simd`, which repeatedly invokes
`collect(storage, step(storage, value))`.

!!! note

    Collection is different from reduction!

    Collection always assumes the order of the collections does not matter, while reduction may
    optionally require that reductions will always be of adjacent values. This makes it easier to
    more efficiently parallelize collections.

    The `collect` function takes a new value and injects it into mutable per-current-thread
    `storage` accumulator(s), while a `reduce` function takes two values and returns a new separate
    value. For example, to perform a sum of integers using `collect`, define the accumulator to be
    an array with a single entry containing the sum.

Having `s_collect` makes it easier to convert a parallel collection to a serial one, for example for
comparing parallel and serial performance.
"""
function s_collect(
    collect::Function,
    step::Function,
    storage::ParallelStorage,
    values;
    simd::SimdFlag = default_simd,
)::Nothing
    s_foreach(storage, values, simd = simd) do storage, value
        collect(storage, step(storage, value))
    end
end

mutable struct ThreadsCollection
    main_thread_id::Int
    pending_thread_id::Int
    remaining_threads_count::Int
    final_thread_id_channel::Channel{Int}
end

"""
    t_collect(collect::Function, step::Function, storage::ParallelStorage, values::collection;
              batch_factor::Int=default_batch_factor, simd::SimdFlag=default_simd)

Perform `step` for each `value` in `values`, in parallel, using multiple threads in the current
process; `collect` the returned results into per-thread storage. When done, collect the separate
per-thread accumulator(s) into the per-current-thread accumulator(s) `storage`, again using multiple
threads.

To manage this, a per-process `_threads_collection` value is used through the collection.

This builds on `s_collect` to accumulate results in each thread. In addition for invoking
`collect(storage, step(...))` in each thread, this will also invoke `collect(from_thread, storage)`
which is expected to collect the per-thread accumulator(s) of `from_thread` into the
per-current-thread accumulator(s) of the `storage`. The order of parameters in this case is
intentionally reversed to allow for deterministic dispatch distinguishing between collecting an
integer step result, and collecting accumulated results from a different thread.

The final `collect` will always be in the current thread so that the per-current-thread
accumulator(s) in the `storage` will contain the final result when `t_collect` returns.
"""
function t_collect(
    collect::Function,
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
        return s_collect(collect, step, storage, values, simd = simd)
    end

    add_per_process!(
        storage,
        "_threads_collection",
        value = ThreadsCollection(
            threadid(),
            0,
            min(batches_count, nthreads()),
            Channel{Int}(1),
        ),
    )

    if batches_count <= nthreads()
        t_collect_up_to_nthreads(
            collect,
            step,
            storage,
            values,
            batch_size,
            batches_count,
            simd,
        )
    else
        t_collect_more_than_nthreads(
            collect,
            step,
            storage,
            values,
            batch_size,
            batches_count,
            simd,
        )
    end

    return nothing
end

function t_collect_up_to_nthreads(
    collect::Function,
    step::Function,
    storage::ParallelStorage,
    values,
    batch_size::Number,
    batches_count::Int,
    simd::SimdFlag,
)::Nothing
    @assert batches_count <= nthreads()

    @sync @threads for batch_index = 1:batches_count
        s_collect(
            collect,
            step,
            storage,
            batch_values_view(values, batch_size, batch_index),
            simd = simd,
        )
        collect_thread_accumulator(collect, storage)
    end

    return nothing
end

function t_collect_more_than_nthreads(
    collect::Function,
    step::Function,
    storage::ParallelStorage,
    values,
    batch_size::Number,
    batches_count::Int,
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
                simd = simd,
            )
            batch_index = atomic_add!(next_batch_index, 1)
        end
        collect_thread_accumulator(collect, storage)
    end

    return nothing
end

# Configuration:

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

    @debug "batches_configuration" batches_count batch_size
    return batches_count, batch_size
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

# Slicing:

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

# Accumulation:

function collect_thread_accumulator(collect::Function, storage::ParallelStorage)::Nothing
    thread_id = threadid()

    # TODO: In theory we could use the main thread for additional collections.
    threads_collection = get_per_process(storage, "_threads_collection")
    if thread_id == threads_collection.main_thread_id
        final_thread_id = take!(threads_collection.final_thread_id_channel)
        collect(final_thread_id, storage)
        return nothing
    end

    did_collect = false
    while true
        other_thread_id = 0
        with_per_process(storage, "_threads_collection") do threads_collection
            if did_collect
                threads_collection.remaining_threads_count -= 1
            end

            other_thread_id = threads_collection.pending_thread_id

            if threads_collection.remaining_threads_count == 2
                @assert other_thread_id == 0
                put!(threads_collection.final_thread_id_channel, thread_id)
            elseif other_thread_id == 0
                threads_collection.pending_thread_id = thread_id
            else
                threads_collection.pending_thread_id = 0
            end
        end

        if other_thread_id == 0
            return nothing
        end

        collect(other_thread_id, storage)
        did_collect = true
    end
end

# Sending:

function send_batches(
    batches_channel::RemoteChannel{Channel{Any}},
    values,
    batch_size::Number,
    first_batch_index::Int,
    last_batch_index::Int,
)::Nothing
    for batch_index = first_batch_index:last_batch_index
        put!(batches_channel, batch_values_view(values, batch_size, batch_index))
    end
end

function send_terminations(
    batches_channel::RemoteChannel{Channel{Any}},
    terminations_count::Int,
)::Nothing
    for _ = 1:terminations_count
        put!(batches_channel, nothing)
    end
    return nothing
end

# Serving:

function s_run_from_batches_channel(
    batches_channel::RemoteChannel{Channel{Any}},
    step::Function,
    storage::ParallelStorage,
    batch_values,
    simd::SimdFlag,
)::Nothing
    while batch_values != nothing
        s_foreach(step, storage, batch_values, simd = simd)
        batch_values = take!(batches_channel)
    end

    return nothing
end

function t_run_from_batches_channel(
    batches_channel::Distributed.RemoteChannel{Channel{Any}},
    threads_count::Int,
    step::Function,
    storage::ParallelStorage,
    batch_values::Array{Any,1},
    simd::SimdFlag,
)::Nothing
    @assert length(batch_values) == threads_count
    @sync @threads for index = 1:threads_count
        s_run_from_batches_channel(
            batches_channel,
            step,
            storage,
            batch_values[index],
            simd,
        )
    end

    return nothing
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

end # module
