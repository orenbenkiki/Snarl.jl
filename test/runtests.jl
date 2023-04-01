using Dates

base_time = now()

using Logging
using Test

using Snarl.DistributedLocks
using Snarl.DistributedLogging

function base_args_contain(value::AbstractString)
    return findfirst(Base.ARGS .== value) != nothing
end

if base_args_contain("--debug") || base_args_contain("-d")
    min_level = Logging.Debug
else
    min_level = Logging.Info
end

macro test_set(args...)
    if length(Base.ARGS) == 0 || base_args_contain("all") || base_args_contain(args[1])
        name = args[1]
        @views block = args[2:length(args)]

        @info name
        forget_locks()
        drain_logging()

        return :(@testset $(name) begin
            $(args...)
            forget_locks()
            drain_logging()
        end)
    end
end

include("launch.jl")
include("locks.jl")
include("control.jl")
