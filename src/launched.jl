"""
Configure once worker processes have been launched.
"""
module Launched

using Base.Threads
using Distributed

export @send_everywhere
export launched
export threads_count_of_processes
export total_threads_count

function run_everywhere(body::Function)::Nothing
    @sync begin
        for worker in workers()
            @spawnat worker body()
        end
        body()
    end

    return nothing
end

"""
    @send_everywhere name value

Define a global variable that will exist on all processes. The initial value will be computed once
on the current (main) process and will be sent to all the other processes. This allows sending
configuration values etc. to all processes.

In tests, when all processes run on the same machine, this also allows sharing values such as
`Atomic` and `SharedArray` between all processes. This will not work in production, where worker
processes run on different machines.
"""
macro send_everywhere(name, value)
    quote
        @everywhere name = nothing
        local value = $(esc(value))
        run_everywhere() do
            global $(esc(name))
            $(esc(name)) = value
        end
    end
end

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
"""
function launched()::Nothing
    myid() == 1 || return nothing

    values = Array{Int,1}(undef, nprocs())

    values[1] = nthreads()
    for worker = 2:nprocs()
        values[worker] = fetch(@spawnat worker Base.Threads.nthreads())
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
