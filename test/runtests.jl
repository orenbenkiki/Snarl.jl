using Distributed
using Test

using Snarl.Launcher
launch_test_workers()

@everywhere using Snarl.Launched
launched()

@everywhere using Base.Threads
@everywhere using Distributed

function test_threads_count_of_processes(
    some_threads_count_of_processes::AbstractArray{Int,1},
)::Nothing
    @test length(some_threads_count_of_processes) == nprocs()

    for threads_count in some_threads_count_of_processes
        @assert threads_count == nthreads()
    end

    return nothing
end

@testset "configuration" begin
    @test nthreads() > 1
    @test nprocs() > 1
    @test nworkers() == test_workers_count

    test_threads_count_of_processes(threads_count_of_processes)
    for worker in workers()
        test_threads_count_of_processes(fetch(@spawnat worker threads_count_of_processes))
    end
end

@everywhere using SharedArrays
@everywhere using Snarl.Resources
@everywhere using Snarl.Control

@everywhere const steps_count = 1000

@send_everywhere next_unique Atomic{Int}(1)

@everywhere function increment_unique!()::Int
    return atomic_add!(next_unique, 1)
end

function reset_unique!()::Nothing
    next_unique[] = 1
    return nothing
end

@testset "next_unique" begin
    reset_unique!()
    @test increment_unique!() == 1
    @test increment_unique!() == 2
end

@everywhere mutable struct OperationContext
    process::Int
    thread::Int
    unique::Int
    resets::Int
    OperationContext() = new(myid(), threadid(), increment_unique!(), 0)
end

@everywhere function increment_resets!(context::OperationContext)::Nothing
    context.resets += 1
    return nothing
end

function new_tracking_array()::AbstractArray{Int,1}
    return SharedArray{Int,1}(steps_count, init = false)
end

@everywhere mutable struct ContextTrackers
    process::AbstractArray{Int,1}
    thread::AbstractArray{Int,1}
    unique::AbstractArray{Int,1}
end

function new_context_trackers()::ContextTrackers
    return ContextTrackers(new_tracking_array(), new_tracking_array(), new_tracking_array())
end

function clear!(context_trackers::ContextTrackers)::Nothing
    fill!(context_trackers.process, 0)
    fill!(context_trackers.thread, 0)
    fill!(context_trackers.unique, 0)
    return nothing
end

@everywhere function track_context(
    context_trackers::ContextTrackers,
    step_index::Int,
    context::OperationContext,
)::Nothing
    @assert context_trackers.process[step_index] == 0
    @assert context_trackers.thread[step_index] == 0
    @assert context_trackers.unique[step_index] == 0

    context_trackers.process[step_index] = context.process
    context_trackers.thread[step_index] = context.thread
    context_trackers.unique[step_index] = context.unique

    return nothing
end

@everywhere mutable struct Trackers
    run_step::ContextTrackers
    per_process::ContextTrackers
    per_thread::ContextTrackers
    per_step::ContextTrackers
    per_step_resets::AbstractArray{Int,1}
end

function new_trackers()::Trackers
    return Trackers(
        new_context_trackers(),
        new_context_trackers(),
        new_context_trackers(),
        new_context_trackers(),
        new_tracking_array(),
    )
end

function clear!(trackers::Trackers)::Nothing
    clear!(trackers.run_step)
    clear!(trackers.per_process)
    clear!(trackers.per_thread)
    clear!(trackers.per_step)
    fill!(trackers.per_step_resets, 0)

    return nothing
end

@send_everywhere trackers new_trackers()

function reset_trackers!()::Nothing
    clear!(trackers)
    return nothing
end

function reset_test!()::Nothing
    reset_unique!()
    reset_trackers!()
    return nothing
end

function track_step(resources::ParallelResources, step_index::Int)::Nothing
    per_step_context = get_per_step(resources, "context")
    @assert per_step_context.resets > 0
    trackers.per_step_resets[step_index] = per_step_context.resets

    track_context(trackers.per_step, step_index, per_step_context)
    track_context(trackers.run_step, step_index, OperationContext())
    track_context(trackers.per_process, step_index, get_per_process(resources, "context"))
    track_context(trackers.per_thread, step_index, get_per_thread(resources, "context"))

    return nothing
end

function test_did_all_steps()::Nothing
    for step = 1:steps_count
        @test trackers.run_step.process[step] > 0
    end

    return nothing
end

function test_same_values(values::AbstractArray, expected::Any)::Nothing
    for actual in values
        @test actual == expected
    end

    return nothing
end

function test_used_single_process(process::Int)::Nothing
    test_same_values(trackers.per_process.process, process)
    test_same_values(trackers.per_thread.process, process)
    test_same_values(trackers.per_step.process, process)
    test_same_values(trackers.run_step.process, process)

    return nothing
end

function test_used_threads_of_single_process(
    expected_used_threads::Int,
    process::Int = myid(),
)::Nothing
    used_threads = Array{Bool,1}(undef, nthreads())
    fill!(used_threads, false)

    test_used_single_process(process)

    for step = 1:steps_count
        thread = trackers.run_step.thread[step]
        @test trackers.per_thread.thread[step] == thread
        @test trackers.per_step.thread[step] == thread
        used_threads[thread] = true
    end

    @test sum(used_threads) == expected_used_threads

    return nothing
end

function test_used_single_thread_of_single_process(process::Int = myid())::Nothing
    thread = trackers.per_thread.thread[1]
    @test thread > 0

    test_used_single_process(process)

    test_same_values(trackers.per_process.thread, thread)
    test_same_values(trackers.per_thread.thread, thread)
    test_same_values(trackers.per_step.thread, thread)
    test_same_values(trackers.run_step.thread, thread)

    return nothing
end

function test_different_unique()::Nothing
    used_uniques = Array{Bool,1}(undef, increment_unique!())
    fill!(used_uniques, false)
    for step_index = 1:steps_count
        unique = trackers.run_step.unique[step_index]
        @test !used_uniques[unique]
        used_uniques[unique] = true
    end
end

function tracking_resources()::ParallelResources
    resources = ParallelResources()
    add_per_process!(resources, "context", make = OperationContext)
    add_per_thread!(resources, "context", make = OperationContext)
    add_per_step!(resources, "context", make = OperationContext, reset = increment_resets!)
    return resources
end

function run_foreach(foreach::Function; flags...)::Nothing
    foreach(track_step, tracking_resources(), 1:steps_count; flags...)
    return nothing
end

function test_t_foreach(; expected_used_threads::Int = nthreads(), flags...)::Nothing
    reset_test!()
    run_foreach(t_foreach; flags...)
    test_did_all_steps()
    test_used_threads_of_single_process(expected_used_threads)
    test_different_unique()
    return nothing
end

@testset "t_foreach" begin
    @testset "default" begin
        test_t_foreach()
    end

    @testset "simd" begin
        @testset "true" begin
            test_t_foreach(batch_simd = true)
        end
        @testset "false" begin
            test_t_foreach(batch_simd = false)
        end
        @testset "invalid" begin
            @test_throws ArgumentError t_foreach(
                track_step,
                tracking_resources(),
                1:1,
                batch_simd = :invalie,
            )
        end
    end

    @testset "thread_batches" begin
        @testset "one" begin
            test_t_foreach(thread_batches = 1)
        end
        @testset "many" begin
            test_t_foreach(thread_batches = typemax(Int))
        end
    end

    @testset "minimal_batch" begin
        @testset "half" begin
            test_t_foreach(expected_used_threads = 2, minimal_batch = div(steps_count, 2))
        end
        @testset "all" begin
            test_t_foreach(expected_used_threads = 1, minimal_batch = typemax(Int))
        end
    end
end
