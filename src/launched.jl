"""
Configure `Snarl` once the worker processes have been launched.
"""
module Launched

using Base.Threads
using Distributed

export launched, threads_count_of_processes, total_threads_count

"""
An array containing the threads count in each of the processes.
"""
threads_count_of_processes = nothing

"""
The total threads count in all the processes.
"""
total_threads_count = 0

"""
    launched()

Configure everything once all the worker processes have been spawned.

Note this does not set up the distributed logging. If you want to use the distributed logger (you
probably do), explicitly invoke `setup_logging` following the call to `launched`.
"""
function launched()::Nothing
    myid() == 1 || return nothing

    values = Array{Int,1}(undef, nprocs())

    @inbounds values[1] = nthreads()
    for worker = 2:nprocs()
        @inbounds values[worker] = fetch(@spawnat worker Base.Threads.nthreads())
    end

    global threads_count_of_processes
    threads_count_of_processes = values
    global total_threads_count
    total_threads_count = sum(values)

    @sync for process = 1:nprocs()
        @spawnat process begin
            global threads_count_of_processes
            threads_count_of_processes = values
            global total_threads_count
            total_threads_count = sum(values)
        end
    end

    return nothing
end

end # module
