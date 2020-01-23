using Distributed
using Snarl.Launcher
launch_test_workers()

@everywhere using Snarl.Launched
launched()

@everywhere using Base.Threads
@everywhere using Distributed

function check_threads_count_of_processes(
    some_threads_count_of_processes::AbstractArray{Int,1},
)::Nothing
    @test length(some_threads_count_of_processes) == nprocs()

    for threads_count in some_threads_count_of_processes
        @assert threads_count == nthreads()
    end

    return nothing
end

@testset "launch" begin
    @test nthreads() > 1
    @test nprocs() > 1
    @test nworkers() == test_workers_count

    check_threads_count_of_processes(threads_count_of_processes)
    for worker in workers()
        check_threads_count_of_processes(fetch(@spawnat worker threads_count_of_processes))
    end
end
