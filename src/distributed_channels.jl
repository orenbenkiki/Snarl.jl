"""
Thread-safe remote (distributed) channels.

In `Snarl`, a remote channel is always used to talk to another process (in general, remote channels
do allow communication within the same process as well). This allows us to simply associate a global
lock with every remote channel instance, and using it to protect every operation; given we never
have both `put!` and `take!` on the same process, this doesn't create a deadlock, and it avoids
the thread-unsafety of the internal `RemoteChannel` implementation.

Thread-safe remote channels should really be part of the `Distributed` package, but this would be
more difficult than this simple implementation, because it will need to deal with `put!` and `take!`
in different threads of the same process. As of writing this, it doesn't seem like this will happen
any time soon. If/when it does, then this code should be deleted and the rest of the code should be
modified to use the standard thread-safe remote channels.
"""
module DistributedChannels

using Base.Threads
using Distributed

import Base: close, put!, fetch, take!, isready

export ThreadSafeRemoteChannel, request_response

"""
    ThreadSafeRemoteChannel(remote_channel, local_lock)

A remote channel for communicating with a different process, together with a lock for coordinating
access to it by threads in the current process.
"""
mutable struct ThreadSafeRemoteChannel{T} <: AbstractChannel{T}
    remote_channel::Union{RemoteChannel{Channel{T}},Nothing}
    local_lock::Union{ReentrantLock,Nothing}
end

function ThreadSafeRemoteChannel(remote_channel::RemoteChannel)::ThreadSafeRemoteChannel
    return ThreadSafeRemoteChannel(remote_channel, ReentrantLock())
end

function close(thread_safe_channel::ThreadSafeRemoteChannel)::Any
    lock(thread_safe_channel.local_lock)  # untested
    try
        return close(thread_safe_channel.remote_channel)  # untested
    finally
        unlock(thread_safe_channel.local_lock)  # untested
    end
end

function put!(thread_safe_channel::ThreadSafeRemoteChannel{T}, value)::Any where {T}
    lock(thread_safe_channel.local_lock)
    try
        return put!(thread_safe_channel.remote_channel, value)
    finally
        unlock(thread_safe_channel.local_lock)
    end
end

function fetch(thread_safe_channel::ThreadSafeRemoteChannel{T}, value)::Any where {T}
    lock(thread_safe_channel.local_lock)  # untested
    try
        return fetch(thread_safe_channel.remote_channel, value)  # untested
    finally
        unlock(thread_safe_channel.local_lock)  # untested
    end
end

function take!(thread_safe_channel::ThreadSafeRemoteChannel{T})::Any where {T}
    lock(thread_safe_channel.local_lock)  # untested
    try
        return take!(thread_safe_channel.remote_channel)  # untested
    finally
        unlock(thread_safe_channel.local_lock)  # untested
    end
end

function isready(thread_safe_channel::ThreadSafeRemoteChannel{T})::Bool where {T}
    lock(thread_safe_channel.local_lock)  # untested
    try
        return isready(thread_safe_channel.remote_channel)  # untested
    finally
        unlock(thread_safe_channel.local_lock)  # untested
    end
end

"""
    request_response(request_channel::Union{Channel,ThreadSafeRemoteChannel},
                     response_channel::Channel)::Union{Channel,RemoteChannel}

Given a `request` channel to send a request through, and a `response` to listen through for the
response, return the properly wrapped response channel to send through the request channel so that
the service at the other side would be able to safely send the response back.

It would be much simpler to send a `Future` through the request channel, but `Future` objects are
not thread safe as of writing this code. If/when they become thread safe, this function should be
removed.
"""
function request_response(;
    request::Union{Channel,ThreadSafeRemoteChannel},
    response::Channel,
)::Union{Channel,RemoteChannel}
    if request isa Channel
        return response
    else
        return RemoteChannel(() -> response)
    end
end

end # module
