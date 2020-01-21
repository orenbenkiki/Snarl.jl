"""
Launch worker processes (typically on other machines).
"""
module Launcher

using Base.Threads
using Distributed

export launch_test_workers
export test_workers_count

const test_workers_count = 4

"""
    launch_test_workers()

Launch a few test worker processes on the same machine.
"""
function launch_test_workers()::Nothing
    Distributed.addprocs(test_workers_count)
    return nothing
end

end # module
