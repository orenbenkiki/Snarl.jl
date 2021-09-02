"""
Provide distributed lock mechanisms.
"""
module DistributedLocks

using Base.Threads
using Distributed

using ..DistributedChannels
using ..Launched

export setup_locks, drain_locks, forget_locks, teardown_locks, with_distributed_lock

local_locks_channel = nothing

struct LockRequest
    scope::String
    operation::Symbol
    response::Union{Channel{Bool},RemoteChannel{Channel{Bool}}}
end

"""
    setup_locks(size=4)::Nothing

This can only be run in the main process, and should be done immediately after launching all the
worker processes. It will spawn a task that serves lock requests arriving on some channel.

The channel allows up to `size` requests to be sent from each thread in each process without
blocking the senders. By default this is set to `4` to minimize the impact of locking on the
senders.

To terminate the task, run `teardown_locks`.
"""
function setup_locks(size::Int = 4)::Nothing
    @assert myid() == 1
    global local_locks_channel
    @assert local_locks_channel == nothing
    local_locks_channel = Channel{Union{Nothing,LockRequest}}(total_threads_count * size)

    if nprocs() > 1
        @sync begin
            remote_locks_channel = RemoteChannel(() -> local_locks_channel)

            for worker_id = 2:nprocs()
                @spawnat worker_id begin
                    global local_locks_channel
                    local_locks_channel = ThreadSafeRemoteChannel(remote_locks_channel)
                end
            end
        end
    end

    @spawnat 1 begin
        global local_locks_channel
        while true
            request = take!(local_locks_channel)
            request != nothing || break
            serve(request)
        end
        local_locks_channel = nothing  # untested
    end

    return nothing
end

"""
    drain_locks()::Nothing

This must be run on the main process. It waits until all lock requests were served. Since lock
requests are sent via a channel to a task that actually serves them (on the main process), it is
useful to be able to ensure that this channel is empty (that is, all locks were served) in certain
points of the program, such as between processing phases (e.g. tests), or before terminating the
program.
"""
function drain_locks()::Nothing
    @assert myid() == 1
    global local_locks_channel
    while local_locks_channel != nothing && isready(local_locks_channel)
        sleep(0.001)  # untested
        yield  # untested
    end
    return nothing
end

"""
    teardown_locks()::Nothing

This must be run on the main process. It terminates the spawned locks task, after serving all the
lock requests that were generated up to this point. Lock requests generated following this (in other
threads) are not guaranteed to be served.
"""
function teardown_locks()::Nothing
    @assert myid() == 1  # untested
    global local_locks_channel  # untested
    @assert local_locks_channel != nothing  # untested
    put!(local_locks_channel, nothing)  # untested
    drain_locks()  # untested
    return nothing  # untested
end

mutable struct LockStatus
    state::Symbol
    pending::Array{Union{Channel{Bool},RemoteChannel{Channel{Bool}}},1}
end

known_locks = Dict{String,LockStatus}()

"""
    forget_locks()::Nothing

This must be run on the main process. This drains all the locks and then forgets all
the previously seen lock scopes (used for testing).
"""
function forget_locks()::Nothing
    drain_locks()
    global known_locks
    known_locks = Dict{String,LockStatus}()
    return nothing
end

function serve(request::LockRequest)::Nothing
    global known_locks
    status = get(known_locks, request.scope, nothing)
    if status == nothing
        status = LockStatus(
            :unlocked,
            Array{Union{Channel{Bool},RemoteChannel{Channel{Bool}}},1}[],
        )
        known_locks[request.scope] = status
        result = true
    else
        result = false
    end

    if request.operation == :lock
        if status.state == :locked
            push!(status.pending, request.response)
        else
            @assert isempty(status.pending)
            status.state = :locked
            put!(request.response, result)
            close(request.response)
        end

    elseif request.operation == :unlock
        @assert status.state == :locked
        put!(request.response, result)
        close(request.response)

        if isempty(status.pending)
            status.state = :unlocked
        else
            response = pop!(status.pending)
            put!(response, result)
            close(response)
        end

    else
        @assert false  # untested
    end

    return nothing
end

"""
    with_distributed_lock(f::Function, scope::String="")::Any

Run the function `f` while holding a distributed lock, passing it a single boolean argument
specifying whether this is the first time the lock has been taken. If `scope` is specified, this
specific lock is obtained. Otherwise, the special global overall lock is obtained. Returns whatever
the function returns.

Note that obtaining a distributed lock is a slow operation compared to locks that are local to the
process. They should therefore only be used to synchronize coarse-grained operations.
"""
function with_distributed_lock(f::Function, scope::String = "")::Any
    global local_locks_channel
    @assert local_locks_channel != nothing
    response = request_response(
        request = local_locks_channel,
        response = Channel{Bool}(100 * myid() + threadid()),
    )
    put!(local_locks_channel, LockRequest(scope, :lock, response))
    yield()
    try
        was_first = take!(response)
        return f(was_first)
    finally
        response = request_response(
            request = local_locks_channel,
            response = Channel{Bool}(100 * myid() + threadid()),
        )
        put!(local_locks_channel, LockRequest(scope, :unlock, response))
        yield()
        take!(response)
    end
end

end # module
