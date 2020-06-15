module Logger

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

export SnarlLogger

"""
    SnarlLogger(stream=stderr; min_level=Info, show_time=true, base_time=false)

A logger which emits only one line per log, in a uniform format, for using in a distributed setting.

If `show_time` is `true`, then each message is prefixed by the current time. If `base_time` is set
to a `DateTime`, the time since that starting point is printed instead. If `base_time` is set to
`true`, the current time is used as a base.
"""
struct SnarlLogger <: AbstractLogger
    stream::IO
    min_level::LogLevel
    show_time::Bool
    base_time::Union{Nothing,DateTime}
    flush::Bool
    message_limits::Dict{Any,Int}
end

SnarlLogger(
    stream::IO = stderr;
    min_level = Info,
    show_time::Bool = true,
    base_time::Union{Bool,DateTime} = false,
    flush::Bool = false,
) = SnarlLogger(stream, min_level, show_time, start_time(base_time), flush, Dict{Any,Int}())

function start_time(base_time::Union{Bool,DateTime})::Union{Nothing,DateTime}
    if base_time isa DateTime  # only appears to be not tested
        return base_time  # only appears to be not tested
    elseif base_time
        return now()  # not tested
    else
        return nothing  # not tested
    end
end

function handle_message(
    logger::SnarlLogger,
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
        remaining = get!(logger.message_limits, id, maxlog)
        logger.message_limits[id] = remaining - 1
        remaining > 0 || return
    end

    buf = IOBuffer()
    iob = IOContext(buf, logger.stream)
    if logger.show_time
        if logger.base_time == nothing
            print(iob, Dates.format(now(), "HH:MM:SS.SSS ; "))  # not tested
        else
            milliseconds = (now() - logger.base_time).value
            seconds, milliseconds = divrem(milliseconds, 1000)
            minutes, seconds = divrem(seconds, 60)
            hours, minutes = divrem(minutes, 60)
            @printf(iob, "%02d:%02d:%02d.%03d ; ", hours, minutes, seconds, milliseconds)
        end
    end

    @printf(iob, "@%02d #%03d %.1s ; ", myid(), threadid(), string(level))
    print(iob, message)

    for (key, val) in kwargs
        print(iob, " ; ", key, " = ", val)
    end

    println(iob)
    write(logger.stream, take!(buf))

    if logger.flush
        flush(logger.stream)
    end

    return nothing
end

shouldlog(logger::SnarlLogger, level, _module, group, id) =
    get(logger.message_limits, id, 1) > 0

min_enabled_level(logger::SnarlLogger) = logger.min_level

catch_exceptions(logger::SnarlLogger) = false

end
