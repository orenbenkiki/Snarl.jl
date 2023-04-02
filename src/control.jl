"""
Provide parallel control flow primitives.
"""
module Control

using Base.Threads
using Distributed

using ..Launched
using ..Storage

export default_batch_factor
export default_maximize_distribution
export default_minimal_batch
export default_simd
export d_foreach
export DistributionPolicy
export dt_foreach
export MaximizeProcesses
export MinimizeProcesses
export next_worker!
export s_foreach
export SimdFlag
export t_foreach

# Data types:

"""
The `@simd` directive to use for an inner loop.

Valid values are `false`, `true` or `:ivdep`.
"""
const SimdFlag = Union{Bool,Symbol}

"""
The policy to use to distribute multi-threaded work across processes.

Specifying `MaximizeProcesses` uses all the processes (servers), by using fewer threads in each one.

Specifying `MinimizeProcesses` uses the fewest processes (servers), by using all the threads in each
one.
"""
@enum DistributionPolicy MaximizeProcesses MinimizeProcesses

# Default parameter values:

"""
The default `@simd` directive to apply to the inner loops.

The default `false` is the conservative choice, as there may be cross-iteration dependencies (which
require explicit coordination, e.g. by using "global" storage in the `ParallelStorage`). Also,
typically when vectorization is a concern, each iteration of the inner loop would contain a nested
vectorized loop using the appropriate `@simd` directive, or use `LoopVectorization`, etc.
"""
const default_simd = false

"""
The default minimal number of steps executed serially in a batch.

The default is `1`, not because it is the most useful, but because no other specific value is
generally useful either.
"""
const default_minimal_batch = 1

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

The default is `MaximizeProcesses` under the assumption that running few threads on each process
(that is, server) will gain from more per-server resources such as memory and I/O bandwidth. It is
also more likely that less threads will run as weaker hyper-threads.
"""
const default_distribution = MaximizeProcesses

# Workers:

next_worker_id = Atomic{Int}(myid())

"""
Return the next worker process to use.

This tries to spread the work evenly between all workers. Mainly intended to be used in `@spawnat
next_worker!() ...` for launching a single job on some process; if at all possible, this will be a
different process than this one.
"""
function next_worker!()::Int
    worker_id = myid()
    if nprocs() > 1
        while worker_id == myid()
            worker_id = 1 + mod(atomic_add!(next_worker_id, 1), nprocs())
        end
    end
    return worker_id
end

function next_workers!(workers_count::Int)::Array{Int,1}
    @assert nprocs() > 1
    @assert 1 <= workers_count && workers_count <= nworkers()
    if workers_count == nworkers()
        worker_ids = [1:nprocs();]
        deleteat!(worker_ids, myid())
        return worker_ids
    end

    worker_ids = Array{Int,1}(undef, workers_count)
    is_process_used = zeros(Bool, nprocs())
    remaining_workers_count = workers_count

    @inbounds while remaining_workers_count > 0
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
    s_foreach(step::Function,
              values::collection;
              storage::Union{ParallelStorage,Nothing}=nothing,
              simd::SimdFlag=default_simd)

Perform `step` for each `value` in `values`, serially, using the current thread in the current
process

This is implemented as a simple loop using the specified `simd`, which repeatedly invokes
`step(value)` or, if `storage` is specified, `step(value, storage)`. The return value of `step` is
discarded.

Having `s_foreach` makes it easier to convert a parallel loop to a serial one, for example for
comparing parallel and serial performance.
"""
function s_foreach(
    step::Function,
    values;
    storage::Union{ParallelStorage,Nothing} = nothing,
    simd::SimdFlag = default_simd,
)::Nothing
    if simd == :ivdep
        if storage == nothing
            @simd ivdep for value in values  # untested
                step(value)  # untested
            end
        else
            @simd ivdep for value in values
                step(value, storage)
            end
        end

    elseif simd == true
        if storage == nothing
            @simd for value in values  # untested
                step(value)  # untested
            end
        else
            @simd for value in values
                step(value, storage)
            end
        end

    elseif simd == false
        if storage == nothing
            for value in values  # untested
                step(value)  # untested
            end
        else
            for value in values
                step(value, storage)
            end
        end

    else
        throw(ArgumentError("Invalid simd flag: $(simd)"))
    end

    yield()  # Allow the scheduler to deal with contention every batch of operations.

    return nothing
end

"""
    t_foreach(step::Function,
              values::collection;
              batch_factor::Int=default_batch_factor,
              minimal_batch::Int=default_minimal_batch,
              storage::Union{ParallelStorage,Nothing}=nothing,
              simd::SimdFlag=default_simd,
              max_threads::Union{Int,Nothing}=nothing,
              finalize_thread::Function=none)

Perform a step for each value in the collection, in parallel, using multiple threads in the current
process. If `max_threads` is specified it must be positive, and the code will use at most this
number of threads.

Scheduling is done in equal-size batches containing at least `minimal_batch` steps in each, where on
average each thread will execute at most `batch_factor` such batches. When each thread completes all
its steps, we invoke `finalize_thread`. If `storage` is not `nothing` we pass it to the
`finalize_thread`.

Each batch is executed as an inner loop using `s_foreach` with the specified `simd`.
"""
function t_foreach(
    step::Function,
    values;
    batch_factor::Int = default_batch_factor,
    minimal_batch::Int = default_minimal_batch,
    storage::Union{ParallelStorage,Nothing} = nothing,
    simd::SimdFlag = default_simd,
    max_threads::Union{Int,Nothing} = nothing,
    finalize_thread::Union{Function,Nothing} = nothing,
)::Nothing
    if max_threads == nothing
        max_threads = nthreads()
    else
        max_threads = min(max_threads, nthreads())  # untested
    end

    if max_threads < 1
        error("Invalid non-positive max_threads $(max_threads)")  # untested
    end

    if max_threads == 1
        s_foreach(step, values, storage = storage, simd = simd)  # untested
        finalize(finalize_thread, storage)  # untested
        return nothing  # untested
    end

    batches_count, batch_size = batches_configuration(
        step,
        length(values),
        batch_factor,
        minimal_batch,
        storage,
        simd,
        max_threads,
    )

    if batches_count <= 1
        s_foreach(step, values, storage = storage, simd = simd)
        finalize(finalize_thread, storage)
    elseif batches_count <= max_threads
        t_foreach_up_to_nthreads(
            step,
            values,
            batch_size,
            batches_count,
            storage,
            simd,
            max_threads,
            finalize_thread,
        )
    else
        t_foreach_more_than_nthreads(
            step,
            values,
            batch_size,
            batches_count,
            storage,
            simd,
            max_threads,
            finalize_thread,
        )
    end

    return nothing
end

"""
    d_foreach(step::Function,
              values::collection;
              batch_factor::Int=default_batch_factor,
              storage::Union{ParallelStorage,Nothing}=nothing,
              simd::SimdFlag=default_simd,
              max_processes::Union{Int,Nothing}=nothing,
              finalize_process::Function=nothing)

Perform a step for each value in the collection, in parallel, using a single thread in each of the
processes (including the current one). If `max_processes` is specified it must be positive, and the
code will use at most this number of processes.

Scheduling is done in equal-size batches containing at least `minimal_batch` steps in each, where on
average each process (thread) will execute at most `batch_factor` such batches. When each process
completes all its steps, we invoke `finalize_process`. If `storage` is not `nothing`, we pass it to
`finalize_process`.

If a `pack` function was specified for any of the per-process values, it is applied to the value
before sending it to the remote processes. Likewise, if a `unpack` function was specified for any of
the per-process values, it is applied to the value when received by the remote process before it is
used by the `step`.

Each batch is executed as an inner loop using `s_foreach` with the specified `simd`.
"""
function d_foreach(
    step::Function,
    values;
    batch_factor::Int = default_batch_factor,
    minimal_batch::Int = default_minimal_batch,
    storage::Union{ParallelStorage,Nothing} = nothing,
    simd::SimdFlag = default_simd,
    max_processes::Union{Int,Nothing} = nothing,
    finalize_process::Union{Function,Nothing} = nothing,
)::Nothing
    if max_processes == nothing
        max_processes = nprocs()
    else
        max_processes = min(max_processes, nprocs())  # untested
    end

    if max_processes < 1
        error("Invalid non-positive max_processes $(max_processes)")  # untested
    end

    if max_processes == 1
        s_foreach(step, values, storage = storage, simd = simd)  # untested
        finalize(finalize_process, storage)  # untested
        return nothing  # untested
    end

    batches_count, batch_size = batches_configuration(
        step,
        length(values),
        batch_factor,
        minimal_batch,
        storage,
        simd,
        max_processes,
    )

    if batches_count <= 1
        s_foreach(step, values, storage = storage, simd = simd)
        finalize(finalize_process, storage)
    elseif batches_count <= max_processes
        d_foreach_up_to_nprocs(
            step,
            values,
            batch_size,
            batches_count,
            storage,
            simd,
            max_processes,
            finalize_process,
        )
    else
        d_foreach_more_than_nprocs(
            step,
            values,
            batch_size,
            batches_count,
            storage,
            simd,
            max_processes,
            finalize_process,
        )
    end

    return nothing
end

"""
    dt_foreach(step::Function,
               values::collection;
               batch_factor::Int=default_batch_factor,
               storage::Union{ParallelStorage,Nothing}=nothing,
               simd::SimdFlag=default_simd,
               max_threads::Union{Int,Nothing} = nothing,
               max_processes::Union{Int,Nothing} = nothing,
               finalize_thread::Function=nothing,
               finalize_process::Function=nothing)

Perform a step for each value in the collection, in parallel, using multiple threads in multiple
processes (including the current one). If `max_processes` is specified it must be positive, and the
code will use at most this number of processes. If `max_threads` is specified it must be positive,
and the code will use at most this number of threads in each process.

Scheduling is done in equal-size batches containing at least `minimal_batch` steps in each,
where on average each thread will execute at most `batch_factor` such batches.

If `distribution` is `maximize_processes`, use all the processes (servers), by using fewer threads
in each one. If it is `minimize_processes`, use the fewest processes (servers), by using all the
threads in each one.

If a `pack` function was specified for any of the per-process values, it is applied to the value
before sending it to the remote processes. Likewise, if a `unpack` function was specified for any of
the per-process values, it is applied to the value when received by the remote process before it is
used by the `step`.

When each thread completes all its steps, we invoke `finalize_thread`. Likewise, when each process
completes all its steps, we invoke `finalize_process`. If `storage` is not `nothing` we pass it to
`finalize_thread` and/or `finalize_process`.

Each batch is executed as an inner loop using `s_foreach` with the specified `simd`.
"""
function dt_foreach(
    step::Function,
    values;
    batch_factor::Int = default_batch_factor,
    minimal_batch::Int = default_minimal_batch,
    distribution::DistributionPolicy = default_distribution,
    storage::Union{ParallelStorage,Nothing} = nothing,
    simd::SimdFlag = default_simd,
    max_threads::Union{Int,Nothing} = nothing,
    max_processes::Union{Int,Nothing} = nothing,
    finalize_thread::Union{Function,Nothing} = nothing,
    finalize_process::Union{Function,Nothing} = nothing,
)::Nothing
    if max_processes == nothing
        max_processes = nprocs()
    else
        max_processes = min(max_processes, nprocs())  # untested
    end

    if max_processes == 1
        t_foreach(  # untested
            step,
            values,
            storage = storage,
            simd = simd,
            max_threads = max_threads,
            finalize_thread = finalize_thread,
        )
        finalize(finalize_process, storage)  # untested
        return nothing  # untested
    end

    if max_processes == nprocs() && max_threads == nothing
        max_runners = total_threads_count
        max_local_threads = nthreads()
        worker_ids = [process for process = 1:nprocs() if process != myid()]
        max_threads_of_workers = @inbounds threads_count_of_processes[worker_ids]
    else
        worker_ids = next_workers!(max_processes - 1)  # untested
        if max_threads == nothing  # untested
            max_local_threads = nthreads()  # untested
            max_threads_of_workers = @inbounds threads_count_of_processes[worker_ids]  # untested
        else
            max_local_threads = min(max_threads, nthreads())  # untested
            max_threads_of_workers =  # untested
                sum(
                    broadcast(
                        min,
                        max_threads,
                        @inbounds threads_count_of_processes[worker_ids]
                    ),
                )
        end
        max_runners = max_local_threads + sum(max_threads_of_workers)  # untested
    end

    batches_count, batch_size = batches_configuration(
        step,
        length(values),
        batch_factor,
        minimal_batch,
        storage,
        simd,
        max_runners,
    )

    if batches_count <= 1
        s_foreach(step, values, storage = storage, simd = simd)
        finalize(finalize_thread, storage)
        finalize(finalize_process, storage)
    elseif distribution == MaximizeProcesses
        dt_foreach_maximize_processes(
            step,
            values,
            batch_size,
            batches_count,
            storage,
            simd,
            finalize_thread,
            finalize_process,
            max_local_threads,
            worker_ids,
            max_threads_of_workers,
        )
    elseif batches_count <= max_local_threads
        t_foreach_up_to_nthreads(
            step,
            values,
            batch_size,
            batches_count,
            storage,
            simd,
            max_local_threads,
            finalize_thread,
        )
        finalize(finalize_process, storage)
    else
        dt_foreach_minimize_processes(
            step,
            values,
            batch_size,
            batches_count,
            storage,
            simd,
            finalize_thread,
            finalize_process,
            max_local_threads,
            worker_ids,
            max_threads_of_workers,
        )
    end

    return nothing
end

function t_foreach_up_to_nthreads(
    step::Function,
    values,
    batch_size::Number,
    batches_count::Int,
    storage::Union{ParallelStorage,Nothing},
    simd::SimdFlag,
    max_threads::Int,
    finalize_thread::Union{Function,Nothing},
)::Nothing
    @assert batches_count <= max_threads

    @sync @threads :static for batch_index = 1:batches_count
        s_foreach(
            step,
            batch_values_view(values, batch_size, batch_index),
            storage = storage,
            simd = simd,
        )
        finalize(finalize_thread, storage)
    end

    return nothing
end

function t_foreach_more_than_nthreads(
    step::Function,
    values,
    batch_size::Number,
    batches_count::Int,
    storage::Union{ParallelStorage,Nothing},
    simd::SimdFlag,
    max_threads::Int,
    finalize_thread::Union{Function,Nothing},
)::Nothing
    @assert batches_count > max_threads

    next_batch_index = Atomic{Int}(max_threads + 1)
    @sync @threads :static for batch_index = 1:max_threads
        while batch_index <= batches_count
            s_foreach(
                step,
                batch_values_view(values, batch_size, batch_index),
                storage = storage,
                simd = simd,
            )
            batch_index = atomic_add!(next_batch_index, 1)
        end
        finalize(finalize_thread, storage)
    end

    return nothing
end

function d_foreach_up_to_nprocs(
    step::Function,
    values,
    batch_size::Number,
    batches_count::Int,
    storage::Union{ParallelStorage,Nothing},
    simd::SimdFlag,
    max_processes::Int,
    finalize_process::Union{Function,Nothing},
)::Nothing
    @assert 1 < batches_count && batches_count <= max_processes

    remote_storage = pack(storage)

    @sync begin
        worker_ids = next_workers!(batches_count - 1)
        for batch_index = 1:(batches_count-1)
            @spawnat (@inbounds worker_ids[batch_index]) begin
                unpack!(remote_storage)

                s_foreach(
                    step,
                    batch_values_view(values, batch_size, batch_index),
                    storage = remote_storage,
                    simd = simd,
                )

                finalize(finalize_process, remote_storage)
                forget!(remote_storage)
            end
        end

        s_foreach(
            step,
            batch_values_view(values, batch_size, batches_count),
            storage = storage,
            simd = simd,
        )

        finalize(finalize_process, storage)
    end

    return nothing
end

function d_foreach_more_than_nprocs(
    step::Function,
    values,
    batch_size::Number,
    batches_count::Int,
    storage::Union{ParallelStorage,Nothing},
    simd::SimdFlag,
    max_processes::Int,
    finalize_process::Union{Function,Nothing},
)::Nothing
    @assert max_processes > 1
    @assert batches_count > max_processes

    local_batches_channel = Channel{Any}(batches_count)
    remote_batches_channel = RemoteChannel(() -> local_batches_channel)
    remote_storage = pack(storage)

    @sync begin
        next_batch_index = 1
        worker_ids = next_workers!(max_processes - 1)
        for worker_id in worker_ids
            @spawnat worker_id begin
                unpack!(remote_storage)

                s_run_from_batches_channel(
                    remote_batches_channel,
                    step,
                    batch_values_view(values, batch_size, next_batch_index),
                    remote_storage,
                    simd,
                )

                finalize(finalize_process, remote_storage)
                forget!(remote_storage)
            end

            next_batch_index += 1
        end

        send_batches(
            remote_batches_channel,
            values,
            batch_size,
            next_batch_index + 1,
            batches_count,
        )
        send_terminations(remote_batches_channel, max_processes)
        close(remote_batches_channel)

        s_run_from_batches_channel(
            local_batches_channel,
            step,
            batch_values_view(values, batch_size, next_batch_index),
            storage,
            simd,
        )

        finalize(finalize_process, storage)
    end

    return nothing
end

function dt_foreach_maximize_processes(
    step::Function,
    values,
    batch_size::Number,
    batches_count::Int,
    storage::Union{ParallelStorage,Nothing},
    simd::SimdFlag,
    finalize_thread::Union{Function,Nothing},
    finalize_process::Union{Function,Nothing},
    max_local_threads::Int,
    worker_ids::Vector{Int},
    max_threads_of_workers::Vector{Int},
)::Nothing
    used_threads_count = min(batches_count, max_local_threads + sum(max_threads_of_workers))
    local_batches_channel = Channel{Any}(batches_count)
    remote_batches_channel = RemoteChannel(() -> local_batches_channel)
    used_threads_of_processes = compute_used_threads_of_processes(
        used_threads_count,
        max_local_threads,
        worker_ids,
        max_threads_of_workers,
    )
    remote_storage = pack(storage)

    next_batch_index = 1
    @sync begin
        for process = 1:nprocs()
            process != myid() || continue
            @inbounds used_threads_of_process = used_threads_of_processes[process]
            used_threads_of_process > 0 || continue
            @spawnat process begin
                unpack!(remote_storage)

                t_run_from_remote_batches_channel(
                    remote_batches_channel,
                    step,
                    batch_values_views(
                        values,
                        batch_size,
                        next_batch_index,
                        used_threads_of_process,
                    ),
                    remote_storage,
                    simd,
                    finalize_thread,
                    used_threads_of_process,
                )

                finalize(finalize_process, remote_storage)
                forget!(remote_storage)
            end
            next_batch_index += used_threads_of_process
        end

        used_threads_of_process = @inbounds used_threads_of_processes[myid()]
        @assert used_threads_of_process > 0
        @threads :static for index = 1:used_threads_of_process
            if index == used_threads_of_process
                send_batches(
                    remote_batches_channel,
                    values,
                    batch_size,
                    next_batch_index + index,
                    batches_count,
                )
                send_terminations(remote_batches_channel, used_threads_count)
                close(remote_batches_channel)
            end

            s_run_from_batches_channel(
                local_batches_channel,
                step,
                batch_values_view(values, batch_size, next_batch_index + index - 1),
                storage,
                simd,
            )

            finalize(finalize_thread, storage)
        end

        finalize(finalize_process, storage)
    end

    return nothing
end

function dt_foreach_minimize_processes(
    step::Function,
    values,
    batch_size::Number,
    batches_count::Int,
    storage::Union{ParallelStorage,Nothing},
    simd::SimdFlag,
    finalize_thread::Union{Function,Nothing},
    finalize_process::Union{Function,Nothing},
    max_local_threads::Int,
    worker_ids::Vector{Int},
    max_threads_of_workers::Vector{Int},
)::Nothing
    remaining_batches_count = batches_count - max_local_threads
    @assert remaining_batches_count > 0

    remote_storage = pack(storage)

    @sync begin
        next_batch_index = 1
        for (worker_id, max_threads_of_worker) in zip(worker_ids, max_threads_of_workers)
            threads_count = min(remaining_batches_count, max_threads_of_worker)
            @assert threads_count > 0

            @spawnat worker_id begin
                unpack!(remote_storage)

                t_run_batches(
                    step,
                    batch_values_views(values, batch_size, next_batch_index, threads_count),
                    remote_storage,
                    simd,
                    finalize_thread,
                )

                finalize(finalize_process, remote_storage)
                forget!(remote_storage)
            end
            next_batch_index += threads_count

            remaining_batches_count -= threads_count
            remaining_batches_count > 0 || break
        end

        t_run_batches(
            step,
            batch_values_views(values, batch_size, next_batch_index, max_local_threads),
            storage,
            simd,
            finalize_thread,
        )

        finalize(finalize_process, storage)

        next_batch_index += nthreads()
        @assert next_batch_index == batches_count + 1
    end
end

# Configuration:

function batches_configuration(
    step::Function,
    values_count::Int,
    batch_factor::Int,
    minimal_batch::Int,
    storage::Union{ParallelStorage,Nothing},
    simd::SimdFlag,
    runners_count::Int,
)::Tuple{Int,Number}
    @assert batch_factor > 0
    @assert minimal_batch > 0
    @assert runners_count > 1

    full_batch_size_per_runner = values_count / runners_count
    small_batch_size_per_runner = full_batch_size_per_runner / batch_factor
    ideal_batch_size = max(small_batch_size_per_runner, minimal_batch)
    batches_count = div(values_count, ideal_batch_size)
    batch_size = values_count / batches_count
    @debug "batches_configuration" batches_count batch_size
    return batches_count, batch_size
end

# TODO: This could be made more efficient if needed.
function compute_used_threads_of_processes(
    used_threads_count::Int,
    max_local_threads::Int,
    worker_ids::Vector{Int},
    max_threads_of_workers::Vector{Int},
)::Array{Int,1}
    used_threads_of_processes = zeros(Int, nprocs())

    remaining_threads_count = used_threads_count
    @assert remaining_threads_count > 0

    @inbounds while true
        for offset = 1:(length(worker_ids)+1)
            if (offset == 1)
                max_threads_of_process = max_local_threads
                process = myid()
            else
                max_threads_of_process = max_threads_of_workers[offset-1]
                process = worker_ids[offset-1]
            end

            if used_threads_of_processes[process] < max_threads_of_process
                used_threads_of_processes[process] += 1
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
    @inbounds for index = 1:batches_count
        views[index] = batch_values_view(values, batch_size, first_batch_index + index - 1)
    end
    return views
end

# Sending:

function send_batches(
    batches_channel::Union{RemoteChannel{Channel{Any}}},
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
    batches_channel::Union{RemoteChannel{Channel{Any}}},
    terminations_count::Int,
)::Nothing
    for _ = 1:terminations_count
        put!(batches_channel, nothing)
    end
    return nothing
end

# Serving:

function s_run_from_batches_channel(
    batches_channel::Union{Channel{Any},RemoteChannel{Channel{Any}}},
    step::Function,
    batch_values,
    storage::Union{ParallelStorage,Nothing},
    simd::SimdFlag,
)::Nothing
    while batch_values != nothing
        s_foreach(step, batch_values, storage = storage, simd = simd)
        batch_values = take!(batches_channel)
    end

    return nothing
end

function t_run_from_remote_batches_channel(
    remote_batches_channel::RemoteChannel{Channel{Any}},
    step::Function,
    batch_values::Array{Any,1},
    storage::Union{ParallelStorage,Nothing},
    simd::SimdFlag,
    finalize_thread::Union{Function,Nothing},
    threads_count::Int,
)::Nothing
    @assert length(batch_values) == threads_count
    @sync @threads :static for index = 1:threads_count
        s_run_from_batches_channel(
            remote_batches_channel,
            step,
            (@inbounds batch_values[index]),
            storage,
            simd,
        )

        finalize(finalize_thread, storage)
    end

    return nothing
end

function t_run_batches(
    step::Function,
    batch_values::Any,
    storage::Union{ParallelStorage,Nothing},
    simd::SimdFlag,
    finalize_thread::Union{Function,Nothing},
)::Nothing
    @sync @threads :static for values in batch_values
        s_foreach(step, values, storage = storage, simd = simd)
        finalize(finalize_thread, storage)
    end

    return nothing
end

function finalize(
    finalizer::Union{Function,Nothing},
    storage::Union{ParallelStorage,Nothing},
)::Nothing
    if finalizer != nothing
        if storage == nothing
            finalizer()  # untested
        else
            finalizer(storage)
        end
    end
    return nothing
end

end # module
