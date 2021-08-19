"""
An opinionated framework for parallel processing.

Julia provides excellent capabilities for running code in multiple threads, and for running code in
multiple processes. However, as far as I could find, it doesn't provide a convenient abstraction for
combining the two. This combination comes up when one wishes to utilize a compute cluster containing
multiple servers, each with multiple processors.

Trying to do this manually is tricky; it even more difficult than it should to be, since as of
writing this, due to historical reasons, Julia's `Distributed` functionality is **not**
`thread`-safe.

`Snarl` tries to provide a simplistic and convenient API for such applications. It is also usable
for purely multi-threaded applications that run in a single process (that is, running an application
on your laptop instead of on your compute cluster servers), and even for pure multi-process
applications (using a single thread on each process). In both cases, these configurations are
considered to be special cases of the general multi-threaded in multi-process configuration.
"""
module Snarl

include("launched.jl")
include("channels.jl")
include("distributed_logging.jl")
include("storage.jl")
include("control.jl")

end # module
