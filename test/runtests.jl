using Logging

global_logger(ConsoleLogger())

using Test

include("affinity.jl")
include("launch.jl")
include("control.jl")
