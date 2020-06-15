using Dates
using Logging
using Test
using Snarl.Logger

base_time = now()
log_level = Logging.Info

global_logger(SnarlLogger(
    stderr,
    min_level = log_level,
    base_time = base_time,
    flush = true,
))

@info "Affinity tests"
include("affinity.jl")

@info "Launch tests"
include("launch.jl")

@info "Control tests"
include("control.jl")
