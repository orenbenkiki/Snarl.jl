"""
Provide improved distributed logging facilities.

The core concept is to allocate a logger thread in the main process and routes all log messages
through it, which serializes the log messages into a single manageable stream.
"""
module DistributedLogging

using Base.Threads
using Dates
using Distributed
using Logging
using Printf

import Base.CoreLogging:
    AbstractLogger,
    SimpleLogger,
    handle_message,
    shouldlog,
    min_enabled_level,
    catch_exceptions

export central_log_channel, DistributedLogger

"""
    central_log_channel(stream=stderr; flush=false, size=4)::RemoteChannel{Channel{Union{Nothing,Array{UInt8,1}}}}

This is expected to run in a thread on the main process. It will listen for log messages arriving on
the channel and write them to the specified stream. If `flush` is set, then the stream is flushed
after writing each message.

Returns the channel it listens on. The size of the channel allows up to `size` messages to be sent
from each process without blocking the senders. By default this is set to `10` to minimize the
impact of logging on the senders.

To terminate the task, send `nothing` to the channel.

In each process (including the main process), the logger needs to be set to a `DistributedLogger`. This
will fully format the message (identifying the process, thread and log level) and send it to the
channel.
"""
function central_log_channel(
    stream = stderr;
    flush::Bool = false,
    size::Int = 10,
)::RemoteChannel{Channel{Union{Nothing,Array{UInt8,1}}}}
    messages_channel =
        RemoteChannel(() -> Channel{Union{Nothing,Array{UInt8,1}}}(nprocs() * size))
    @spawnat myid() begin
        while true
            message = take!(messages_channel)
            message != nothing || break
            write(stream, message)
            flush && Base.flush(stream)
            yield()
        end
    end
    return messages_channel
end

"""
    DistributedLogger(log_channel; min_level=Info, show_time=true, base_time=nothing)

A logger which emits only one line per log, in a uniform format, for using in a distributed setting.

If `show_time` is `true`, then each message is prefixed by the current time. If `base_time` is set
to a `DateTime`, the time since that starting point is printed instead.
"""
struct DistributedLogger <: AbstractLogger
    log_channel::RemoteChannel{Channel{Union{Nothing,Array{UInt8,1}}}}
    min_level::LogLevel
    show_time::Bool
    base_time::Union{Nothing,DateTime}
    message_limits::Dict{Any,Int}
end

DistributedLogger(
    log_channel::RemoteChannel{Channel{Union{Nothing,Array{UInt8,1}}}};
    min_level::LogLevel = Info,
    show_time::Bool = true,
    base_time::Union{Nothing,DateTime} = nothing,
) = DistributedLogger(log_channel, min_level, show_time, base_time, Dict{Any,Int}())

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

    @printf(iob, "@%02d #%03d ", myid(), threadid())
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

min_enabled_level(logger::DistributedLogger) = logger.min_level

catch_exceptions(logger::DistributedLogger) = false

end
