"""
Provide parallel control flow primitives.
"""
module Control

using Base.Threads
using Distributed

using ..DistributedChannels
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

This is implemented as a simple loop using the specified `simd`, which repeatedly invokes
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
              batch_factor::Int=default_batch_factor,
              minimal_batch::Int=default_minimal_batch,
              simd::SimdFlag=default_simd,
              finalize_thread::Function=none)

Perform a step for each value in the collection, in parallel, using multiple threads in the current
process.

Scheduling is done in equal-size batches containing at least `minimal_batch` steps in each, where on
average each thread will execute at most `batch_factor` such batches. When each thread completes all
its steps, we invoke `finalize_thread` and pass it the `storage`.

Each batch is executed as an inner loop using `s_foreach` with the specified `simd`.
"""
function t_foreach(
    step::Function,
    storage::ParallelStorage,
    values;
    batch_factor::Int = default_batch_factor,
    minimal_batch::Int = default_minimal_batch,
    simd::SimdFlag = default_simd,
    finalize_thread::Union{Function,Nothing} = nothing,
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
        if finalize_thread != nothing
            finalize_thread(storage)
        end
    elseif batches_count <= nthreads()
        t_foreach_up_to_nthreads(
            step,
            storage,
            values,
            batch_size,
            batches_count,
            simd,
            finalize_thread,
        )
    else
        t_foreach_more_than_nthreads(
            step,
            storage,
            values,
            batch_size,
            batches_count,
            simd,
            finalize_thread,
        )
    end

    return nothing
end

"""
    d_foreach(step::Function, storage::ParallelStorage, values::collection;
              batch_factor::Int=default_batch_factor, simd::SimdFlag=default_simd,
              channel_names::Union{String,collection{String},Nothing}=nothing,
              finalize_process::Function=nothing)

Perform a step for each value in the collection, in parallel, using a single thread in each of the
processes (including the current one).

Scheduling is done in equal-size batches containing at least `minimal_batch` steps in each, where on
average each process (thread) will execute at most `batch_factor` such batches. When each process
completes all its steps, we invoke `finalize_process` and pass it the `storage`.

If `channel_names` is specified then it should list one or more names of pre-process `Channel`
values. These values are automatically converted to `ThreadSafeRemoteChannel` values on the remote
processes so they are safe to use in a multi-threaded application.

Each batch is executed as an inner loop using `s_foreach` with the specified `simd`.
"""
function d_foreach(
    step::Function,
    storage::ParallelStorage,
    values;
    batch_factor::Int = default_batch_factor,
    minimal_batch::Int = default_minimal_batch,
    simd::SimdFlag = default_simd,
    channel_names = nothing,
    finalize_process::Union{Function,Nothing} = nothing,
)::Nothing
    if nprocs() == 1
        s_foreach(step, storage, values, simd = simd)  # untested
        if finalize_process != nothing  # untested
            finalize_process(storage)  # untested
        end
        return nothing  # untested
    end

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
        if finalize_process != nothing
            finalize_process(storage)
        end
    elseif batches_count <= nprocs()
        d_foreach_up_to_nprocs(
            step,
            storage,
            values,
            batch_size,
            batches_count,
            simd,
            channel_names,
            finalize_process,
        )
    else
        d_foreach_more_than_nprocs(
            step,
            storage,
            values,
            batch_size,
            batches_count,
            simd,
            channel_names,
            finalize_process,
        )
    end

    return nothing
end

"""
    dt_foreach(step::Function, storage::ParallelStorage, values::collection;
               batch_factor::Int=default_batch_factor, simd::SimdFlag=default_simd,
               channel_names::Union{String,collection{String},Nothing}=nothing,
               finalize_process::Function=nothing)

Perform a step for each value in the collection, in parallel, using multiple threads in multiple
processes (including the current one).

Scheduling is done in equal-size batches containing at least `minimal_batch` steps in each,
where on average each thread will execute at most `batch_factor` such batches.

If `distribution` is `maximize_processes`, use all the processes (servers), by using fewer threads
in each one. If it is `minimize_processes`, use the fewest processes (servers), by using all the
threads in each one.

If `channel_names` is specified then it should list one or more names of pre-process `Channel`
values. These values are automatically converted to `ThreadSafeRemoteChannel` values on the remote
processes so they are safe to use in a multi-threaded application.

When each thread completes all its steps, we invoke `finalize_thread` and pass it the `storage`.
Likewise, when each process completes all its steps, we invoke `finalize_process` and pass it the
`storage`.

Each batch is executed as an inner loop using `s_foreach` with the specified `simd`.
"""
function dt_foreach(
    step::Function,
    storage::ParallelStorage,
    values;
    batch_factor::Int = default_batch_factor,
    minimal_batch::Int = default_minimal_batch,
    distribution::DistributionPolicy = default_distribution,
    simd::SimdFlag = default_simd,
    channel_names = nothing,
    finalize_thread::Union{Function,Nothing} = nothing,
    finalize_process::Union{Function,Nothing} = nothing,
)::Nothing
    if nprocs() == 1
        s_foreach(step, storage, values, simd = simd)  # untested

        if finalize_thread != nothing  # untested
            finalize_thread(storage)  # untested
        end
        if finalize_process != nothing  # untested
            finalize_process(storage)  # untested
        end

        return nothing  # untested
    end

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

        if finalize_thread != nothing
            finalize_thread(storage)  # untested
        end
        if finalize_process != nothing
            finalize_process(storage)
        end
    elseif distribution == MaximizeProcesses
        dt_foreach_maximize_processes(
            step,
            storage,
            values,
            batch_size,
            batches_count,
            simd,
            channel_names,
            finalize_thread,
            finalize_process,
        )
    elseif batches_count <= nthreads()
        t_foreach_up_to_nthreads(
            step,
            storage,
            values,
            batch_size,
            batches_count,
            simd,
            finalize_thread,
        )

        if finalize_process != nothing
            finalize_process(storage)
        end
    else
        dt_foreach_minimize_processes(
            step,
            storage,
            values,
            batch_size,
            batches_count,
            simd,
            channel_names,
            finalize_thread,
            finalize_process,
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
    simd::SimdFlag,
    finalize_thread::Union{Function,Nothing},
)::Nothing
    @assert batches_count <= nthreads()

    @sync @threads for batch_index = 1:batches_count
        s_foreach(
            step,
            storage,
            batch_values_view(values, batch_size, batch_index),
            simd = simd,
        )

        if finalize_thread != nothing
            finalize_thread(storage)  # untested
        end
    end

    return nothing
end

function t_foreach_more_than_nthreads(
    step::Function,
    storage::ParallelStorage,
    values,
    batch_size::Number,
    batches_count::Int,
    simd::SimdFlag,
    finalize_thread::Union{Function,Nothing},
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
        if finalize_thread != nothing
            finalize_thread(storage)  # untested
        end
    end

    return nothing
end

function collect_remote_results_channels(
    storage::ParallelStorage,
    channel_names,
)::Union{Dict{String,RemoteChannel},Nothing}
    if channel_names == nothing
        return nothing  # untested
    end

    remote_results_channels = Dict{String,RemoteChannel}()
    if isa(channel_names, String)
        local_results_channel = get_per_process(storage, channel_names)
        remote_results_channels[channel_names] = RemoteChannel(() -> local_results_channel)
    else
        for channel_name in channel_names
            local_results_channel = get_per_process(storage, channel_name)
            remote_results_channels[channel_name] =
                RemoteChannel(() -> local_results_channel)
        end
    end

    return remote_results_channels
end

function inject_remote_results_channels(
    storage::ParallelStorage,
    remote_results_channels::Union{Dict{String,RemoteChannel},Nothing},
)::Nothing
    if remote_results_channels == nothing
        return nothing  # untested
    end
    for (channel_name, remote_results_channel) in remote_results_channels
        storage.per_process[channel_name].value =
            ThreadSafeRemoteChannel(remote_results_channel)
    end
end

function d_foreach_up_to_nprocs(
    step::Function,
    storage::ParallelStorage,
    values,
    batch_size::Number,
    batches_count::Int,
    simd::SimdFlag,
    channel_names,
    finalize_process::Union{Function,Nothing},
)::Nothing
    @assert 1 < batches_count && batches_count <= nprocs()

    remote_results_channels = collect_remote_results_channels(storage, channel_names)

    @sync begin
        worker_ids = next_workers!(batches_count - 1)
        for batch_index = 1:(batches_count-1)
            @spawnat worker_ids[batch_index] begin
                inject_remote_results_channels(storage, remote_results_channels)

                s_foreach(
                    step,
                    storage,
                    batch_values_view(values, batch_size, batch_index),
                    simd = simd,
                )

                if finalize_process != nothing
                    finalize_process(storage)
                end

                forget!(storage)
            end
        end

        s_foreach(
            step,
            storage,
            batch_values_view(values, batch_size, batches_count),
            simd = simd,
        )

        if finalize_process != nothing
            finalize_process(storage)
        end
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
    channel_names,
    finalize_process::Union{Function,Nothing},
)::Nothing
    @assert batches_count > nprocs()

    local_batches_channel = Channel{Any}(batches_count)
    remote_batches_channel = RemoteChannel(() -> local_batches_channel)
    remote_results_channels = collect_remote_results_channels(storage, channel_names)

    @sync begin
        next_batch_index = 1
        worker_ids = next_workers!(nworkers())
        for worker_id in worker_ids
            @spawnat worker_id begin
                inject_remote_results_channels(storage, remote_results_channels)

                s_run_from_batches_channel(
                    remote_batches_channel,
                    step,
                    storage,
                    batch_values_view(values, batch_size, next_batch_index),
                    simd,
                )

                if finalize_process != nothing
                    finalize_process(storage)
                end

                forget!(storage)
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
        send_terminations(remote_batches_channel, nprocs())
        close(remote_batches_channel)

        s_run_from_batches_channel(
            local_batches_channel,
            step,
            storage,
            batch_values_view(values, batch_size, next_batch_index),
            simd,
        )

        if finalize_process != nothing
            finalize_process(storage)
        end
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
    channel_names,
    finalize_thread::Union{Function,Nothing},
    finalize_process::Union{Function,Nothing},
)::Nothing
    used_threads_count = min(batches_count, total_threads_count)
    local_batches_channel = Channel{Any}(batches_count)
    remote_batches_channel = RemoteChannel(() -> local_batches_channel)
    used_threads_of_processes = compute_used_threads_of_processes(used_threads_count)
    remote_results_channels = collect_remote_results_channels(storage, channel_names)

    next_batch_index = 1
    @sync begin
        for process = 1:nprocs()
            process != myid() || continue
            @inbounds used_threads_of_process = used_threads_of_processes[process]
            used_threads_of_process > 0 || continue
            @spawnat process begin
                inject_remote_results_channels(storage, remote_results_channels)

                t_run_from_remote_batches_channel(
                    remote_batches_channel,
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
                    finalize_thread,
                )

                if finalize_process != nothing
                    finalize_process(storage)
                end

                forget!(storage)
            end
            next_batch_index += used_threads_of_process
        end

        used_threads_of_process = @inbounds used_threads_of_processes[myid()]
        @assert used_threads_of_process > 0
        @threads for index = 1:used_threads_of_process
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
                storage,
                batch_values_view(values, batch_size, next_batch_index + index - 1),
                simd,
            )

            if finalize_thread != nothing
                finalize_thread(storage)  # untested
            end
        end

        if finalize_process != nothing
            finalize_process(storage)
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
    channel_names,
    finalize_thread::Union{Function,Nothing},
    finalize_process::Union{Function,Nothing},
)::Nothing
    used_processes = zeros(Bool, nprocs())
    used_processes[myid()] = true

    remaining_batches_count = batches_count - nthreads()
    @assert remaining_batches_count > 0

    remote_results_channels = collect_remote_results_channels(storage, channel_names)

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

            @spawnat worker_id begin
                inject_remote_results_channels(storage, remote_results_channels)

                t_run_batches(
                    step,
                    storage,
                    batch_values_views(values, batch_size, next_batch_index, threads_count),
                    simd,
                    finalize_thread,
                )

                if finalize_process != nothing
                    finalize_process(storage)
                end

                forget!(storage)
            end
            next_batch_index += threads_count

            remaining_batches_count -= threads_count
            remaining_batches_count > 0 || break
        end

        t_run_batches(
            step,
            storage,
            batch_values_views(values, batch_size, next_batch_index, nthreads()),
            simd,
            finalize_thread,
        )

        if finalize_process != nothing
            finalize_process(storage)
        end

        next_batch_index += nthreads()
        @assert next_batch_index == batches_count + 1
    end
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
    batches_channel::Union{
        Channel{Any},
        RemoteChannel{Channel{Any}},
        ThreadSafeRemoteChannel{Any},
    },
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

function t_run_from_remote_batches_channel(
    remote_batches_channel::RemoteChannel{Channel{Any}},
    threads_count::Int,
    step::Function,
    storage::ParallelStorage,
    batch_values::Array{Any,1},
    simd::SimdFlag,
    finalize_thread::Union{Function,Nothing},
)::Nothing
    general_batches_channel = ThreadSafeRemoteChannel(remote_batches_channel)
    @assert length(batch_values) == threads_count
    @sync @threads for index = 1:threads_count
        s_run_from_batches_channel(
            general_batches_channel,
            step,
            storage,
            batch_values[index],
            simd,
        )

        if finalize_thread != nothing
            finalize_thread(storage)  # untested
        end
    end

    return nothing
end

function t_run_batches(
    step::Function,
    storage::ParallelStorage,
    batch_values::Any,
    simd::SimdFlag,
    finalize_thread::Union{Function,Nothing},
)::Nothing
    @sync @threads for index = 1:length(batch_values)
        s_foreach(step, storage, batch_values[index], simd = simd)

        if finalize_thread != nothing
            finalize_thread(storage)  # untested
        end
    end

    return nothing
end

end # module
