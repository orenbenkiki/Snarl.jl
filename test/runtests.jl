using Dates
using Logging
using Test
using Snarl.Logger

function base_args_contain(value::AbstractString)
    return findfirst(Base.ARGS .== value) != nothing
end

macro test_set(args...)
    if length(Base.ARGS) == 0 || base_args_contain(args[1])
        @info args[1]
        return :(@testset $(args...))
    end
end

base_time = now()

if base_args_contain("--debug") || base_args_contain("-d")
    log_level = Logging.Debug
else
    log_level = Logging.Info
end

global_logger(SnarlLogger(
    stderr,
    min_level = log_level,
    base_time = base_time,
    flush = true,
))

include("affinity.jl")
include("launch.jl")
include("control.jl")
