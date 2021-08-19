"""
Provide improved distributed logging facilities.

The core concept is to have a logger task in the main process and routes all log messages through
it, which serializes the log messages into a single manageable stream.
"""
module DistributedLogging

using Base.Threads
using Dates
using Distributed
using Logging
using Printf

using ..Channels
using ..Launched

import Base.CoreLogging:
    AbstractLogger,
    SimpleLogger,
    handle_message,
    shouldlog,
    min_enabled_level,
    catch_exceptions

export setup_logging, drain_logging, teardown_logging, DistributedLogger

local_log_channel = nothing

"""
    setup_logging(stream=stderr; flush=false, size=4)::RemoteChannel{Channel{Union{Nothing,Array{UInt8,1}}}}

This can only be run in the main process, and should be done immediately after launching all the
worker processes. It will spawn a task that listens for log messages arriving on some channel, and
write them to the specified stream.

If `show_time` is `true`, then each message is prefixed by the current time. If `base_time` is set
to a `DateTime`, the time since that starting point is printed instead. If `flush` is set, then the
stream is flushed after writing each message.

The channel allows up to `size` messages to be sent from each thread in each process without
blocking the senders. By default this is set to `4` to minimize the impact of logging on the
senders.

To terminate the task, run `teardown_logging`.
"""
function setup_logging(
    stream = stderr;
    min_level::LogLevel = Logging.Warn,
    show_time::Bool = false,
    base_time::Union{Nothing,DateTime} = Nothing,
    flush::Bool = false,
    size::Int = 4,
)
    @assert myid() == 1
    global local_log_channel
    @assert local_log_channel == nothing
    local_log_channel = Channel{Union{Nothing,Array{UInt8,1}}}(total_threads_count * size)

    global_logger(
        DistributedLogger(
            local_log_channel,
            min_level,
            show_time,
            base_time,
            Dict{Any,Int}(),
        ),
    )

    if nprocs() > 1
        @sync begin
            remote_log_channel = RemoteChannel(() -> local_log_channel)

            for worker_id = 2:nprocs()
                @spawnat worker_id begin
                    global_logger(
                        DistributedLogger(
                            ThreadSafeRemoteChannel(remote_log_channel),
                            min_level,
                            show_time,
                            base_time,
                            Dict{Any,Int}(),
                        ),
                    )
                end
            end
        end
    end

    @spawnat 1 begin
        global local_log_channel
        while true
            message = take!(local_log_channel)
            message != nothing || break
            write(stream, message)
            flush && Base.flush(stream)
            yield()
        end
        local_log_channel = nothing  # untested
    end
end

"""
    drain_logging()

This must be run on the main process. It waits until all log messages were printed. Since log
messages are sent via a channel to a task that actually prints them (on the main process), it is
useful to be able to ensure that this channel is empty (that is, all messages were printed) in
certain points of the program, such as between processing phases (e.g. tests), or before terminating
the program.
"""
function drain_logging()
    @assert myid() == 1
    global local_log_channel
    while local_log_channel != nothing && isready(local_log_channel)
        sleep(0.001)
        yield
    end
end

"""
    teardown_logging()

This must be run on the main process. It terminates the spawned logging task, after printing all the
log messages that were generated up to this point. Log messages generated following this (in other
threads) are not guaranteed to be printed.
"""
function teardown_logging()
    @assert myid() == 1
    global local_log_channel
    @assert local_log_channel != nothing
    put!(local_log_channel, nothing)
    drain_logging()
end

"""
    DistributedLogger(log_channel, min_level, show_time, base_time, message_limits)

A logger which emits only one line per log, in a uniform format, for use in a distributed setting.
"""
struct DistributedLogger <: AbstractLogger
    log_channel::AbstractChannel{Union{Nothing,Array{UInt8,1}}}
    min_level::LogLevel
    show_time::Bool
    base_time::Union{Nothing,DateTime}
    message_limits::Dict{Any,Int}
end

function handle_message(
    logger::DistributedLogger,
    level,
    message,
    _module,
    group,
    id,
    filepath,
    line;
    maxlog = nothing,
    kwargs...,
)::Nothing
    if maxlog !== nothing && maxlog isa Integer
        remaining = get!(logger.message_limits, id, maxlog) # untested
        logger.message_limits[id] = remaining - 1 # untested
        remaining > 0 || return # untested
    end

    buf = IOBuffer()
    iob = IOContext(buf)
    if logger.show_time
        if logger.base_time == nothing
            print(iob, Dates.format(now(), "HH:MM:SS.SSS ; "))  # untested
        else
            milliseconds = (now() - logger.base_time).value
            seconds, milliseconds = divrem(milliseconds, 1000)
            minutes, seconds = divrem(seconds, 60)
            hours, minutes = divrem(minutes, 60)
            @printf(iob, "%02d:%02d:%02d.%03d ; ", hours, minutes, seconds, milliseconds)
        end
    end

    @printf(iob, "@%03d #%03d ", myid(), threadid())
    if level == Logging.Debug
        printstyled(iob, "D", color = Base.debug_color())  # untested
    elseif level == Logging.Info
        printstyled(iob, "I", color = Base.info_color())
    elseif level == Logging.Warn  # untested
        printstyled(iob, "W", color = Base.warn_color())  # untested
    elseif level == Logging.Error  # untested
        printstyled(iob, "E", color = Base.error_color())  # untested
    else
        @assert false  # untested
    end

    print(iob, " ; ", message)

    for (key, val) in kwargs
        print(iob, " ; ", key, " = ", val)  # untested
    end

    println(iob)
    put!(logger.log_channel, take!(buf))

    return nothing
end

shouldlog(logger::DistributedLogger, level, _module, group, id) =
    get(logger.message_limits, id, 1) > 0

min_enabled_level(logger::DistributedLogger) = begin
    return logger.min_level
end

catch_exceptions(logger::DistributedLogger) = false

end
