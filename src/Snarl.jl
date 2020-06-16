"""
An opinionated framework for parallel processing.
"""
module Snarl

include("distributed_logging.jl")
include("launcher.jl")
include("launched.jl")
include("storage.jl")
include("control.jl")

end # module
