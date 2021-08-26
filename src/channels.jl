"""
Thread-safe remote channels.

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
module Channels

using Base.Threads
using Distributed

import Base: close, put!, fetch, take!, isready

export ThreadSafeRemoteChannel

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

function close(general_channel::ThreadSafeRemoteChannel)
    lock(general_channel.local_lock)
    try
        close(general_channel.remote_channel)
    finally
        unlock(general_channel.local_lock)
    end
end

function put!(general_channel::ThreadSafeRemoteChannel{T}, value) where {T}
    lock(general_channel.local_lock)
    try
        put!(general_channel.remote_channel, value)
    finally
        unlock(general_channel.local_lock)
    end
end

function fetch(general_channel::ThreadSafeRemoteChannel{T}, value) where {T}
    lock(general_channel.local_lock)
    try
        fetch(general_channel.remote_channel, value)
    finally
        unlock(general_channel.local_lock)
    end
end

function take!(general_channel::ThreadSafeRemoteChannel{T}) where {T}
    lock(general_channel.local_lock)
    try
        return take!(general_channel.remote_channel)
    finally
        unlock(general_channel.local_lock)
    end
end

function isready(general_channel::ThreadSafeRemoteChannel{T}) where {T}
    lock(general_channel.local_lock)
    try
        return isready(general_channel.remote_channel)
    finally
        unlock(general_channel.local_lock)
    end
end

end # module
