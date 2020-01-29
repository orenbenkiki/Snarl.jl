using Distributed
using Snarl.Launcher
launch_test_workers()

@everywhere using Snarl.Launched
launched()

@everywhere using Base.Threads
@everywhere using Distributed

@everywhere function run_query()::Tuple{Int,AbstractArray{Int,1}}
    return nprocs(), threads_count_of_processes
end

function check_query_results(query_result::Tuple{Int,AbstractArray{Int,1}})::Nothing
    processes_count = query_result[1]
    some_threads_count_of_processes = query_result[2]

    @test processes_count == nprocs()
    @test length(some_threads_count_of_processes) == nprocs()

    for threads_count in some_threads_count_of_processes
        @test threads_count == nthreads()
    end

    @test total_threads_count == sum(threads_count_of_processes)

    return nothing
end

@testset "launch" begin
    @test nthreads() > 1
    @test nprocs() > 1
    @test nworkers() == test_workers_count

    check_query_results(run_query())
    for process_id = 1:nprocs()
        check_query_results(fetch(@spawnat process_id run_query()))
    end
end
