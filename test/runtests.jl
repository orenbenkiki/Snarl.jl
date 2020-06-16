using Dates
using Logging
using Test
using Snarl.DistributedLogging

function base_args_contain(value::AbstractString)
    return findfirst(Base.ARGS .== value) != nothing
end

log_channel = central_log_channel(flush = true)

function drain_log_channel()
    while isready(log_channel)
        sleep(0.001)
        yield
    end
end

macro test_set(args...)
    if length(Base.ARGS) == 0 || base_args_contain("all") || base_args_contain(args[1])
        name = args[1]
        @views block = args[2:length(args)]

        @info name
        drain_log_channel()

        return :(@testset $(name) begin
            $(args...)
            drain_log_channel()
        end)
    end
end

base_time = now()

if base_args_contain("--debug") || base_args_contain("-d")
    log_level = Logging.Debug
else
    log_level = Logging.Info
end

include("affinity.jl")
include("launch.jl")
include("control.jl")
