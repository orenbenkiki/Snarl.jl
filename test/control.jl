@everywhere using SharedArrays
@everywhere using Snarl.Resources
@everywhere using Snarl.Control

const steps_count = 1000

unused_unique = Atomic{Int}(1)

function increment_main_unique()::Int
    return atomic_add!(unused_unique, 1)
end

function last_unique()::Int
    return unused_unique[] - 1
end

@everywhere function next_unique!()::Int
    return @fetchfrom 1 increment_main_unique()
end

function reset_unique!()::Nothing
    next_unique[] = 1
    return nothing
end

function reset_unique!()::Nothing
    unused_unique[] = 1
    return nothing
end

@everywhere mutable struct OperationContext
    process::Int
    thread::Int
    unique::Int
    resets::Int
    OperationContext() = new(myid(), threadid(), next_unique!(), 0)
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

@everywhere function track_step(resources::ParallelResources, step_index::Int)::Nothing
    per_step_context = get_per_step(resources, "context")
    @assert per_step_context.resets > 0
    trackers.per_step_resets[step_index] = per_step_context.resets

    track_context(trackers.per_step, step_index, per_step_context)
    track_context(trackers.run_step, step_index, OperationContext())
    track_context(trackers.per_process, step_index, get_per_process(resources, "context"))
    track_context(trackers.per_thread, step_index, get_per_thread(resources, "context"))

    return nothing
end

function check_did_all_steps()::Nothing
    for step = 1:steps_count
        @test trackers.run_step.process[step] > 0
    end

    return nothing
end

function check_same_values(values::AbstractArray, expected::Any)::Nothing
    for actual in values
        @test actual == expected
    end

    return nothing
end

function check_used_single_process(process::Int)::Nothing
    check_same_values(trackers.per_process.process, process)
    check_same_values(trackers.per_thread.process, process)
    check_same_values(trackers.per_step.process, process)
    check_same_values(trackers.run_step.process, process)

    return nothing
end

function check_used_threads_of_single_process(
    expected_used_threads::Int,
    process::Int = myid(),
)::Nothing
    used_threads = Array{Bool,1}(undef, nthreads())
    fill!(used_threads, false)

    check_used_single_process(process)

    for step = 1:steps_count
        thread = trackers.run_step.thread[step]
        @test trackers.per_thread.thread[step] == thread
        @test trackers.per_step.thread[step] == thread
        used_threads[thread] = true
    end

    @test sum(used_threads) == expected_used_threads

    return nothing
end

function check_different_unique()::Nothing
    used_uniques = Array{Bool,1}(undef, last_unique())
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

function check_used_single_thread_of_single_process(process::Int = myid())::Nothing
    thread = trackers.per_thread.thread[1]
    @test thread > 0

    check_used_single_process(process)

    check_same_values(trackers.per_process.thread, thread)
    check_same_values(trackers.per_thread.thread, thread)
    check_same_values(trackers.per_step.thread, thread)
    check_same_values(trackers.run_step.thread, thread)

    return nothing
end

function check_s_foreach(; flags...)::Nothing
    reset_test!()
    run_foreach(s_foreach; flags...)
    check_did_all_steps()
    check_used_threads_of_single_process(1)
    check_different_unique()
    return nothing
end

@testset "s_foreach" begin
    @testset "default" begin
        check_s_foreach()
    end

    @testset "simd" begin
        @testset "true" begin
            check_s_foreach(simd = true)
        end
        @testset "false" begin
            check_s_foreach(simd = false)
        end
        @testset "invalid" begin
            @test_throws ArgumentError s_foreach(
                track_step,
                tracking_resources(),
                1:1,
                simd = :invalid,
            )
        end
    end
end

function check_t_foreach(; expected_used_threads::Int = nthreads(), flags...)::Nothing
    reset_test!()
    run_foreach(t_foreach; flags...)
    check_did_all_steps()
    check_used_threads_of_single_process(expected_used_threads)
    check_different_unique()
    return nothing
end

@testset "t_foreach" begin
    @testset "default" begin
        check_t_foreach()
    end

    @testset "batch_factor" begin
        @testset "one" begin
            check_t_foreach(batch_factor = 1)
        end
        @testset "many" begin
            check_t_foreach(batch_factor = typemax(Int))
        end
    end

    @testset "minimal_batch" begin
        @testset "half" begin
            check_t_foreach(expected_used_threads = 2, minimal_batch = div(steps_count, 2))
        end
        @testset "all" begin
            check_t_foreach(expected_used_threads = 1, minimal_batch = typemax(Int))
        end
    end
end

function check_used_single_thread_of_processes(
    expected_used_processes::Int = nproc(),
)::Nothing
    thread_of_processes = Array{Int,1}(undef, nprocs())
    fill!(thread_of_processes, 0)
    used_processes = 0

    for step = 1:steps_count
        process = trackers.run_step.process[step]
        thread = trackers.run_step.thread[step]
        @test trackers.per_thread.thread[step] == thread
        @test trackers.per_step.thread[step] == thread
        if thread_of_processes[process] == 0
            thread_of_processes[process] = thread
            used_processes += 1
        else
            @test thread == thread_of_processes[process]
        end
    end

    @test used_processes == expected_used_processes

    return nothing
end

function check_different_unique()::Nothing
    used_uniques = Array{Bool,1}(undef, last_unique())
    fill!(used_uniques, false)
    for step_index = 1:steps_count
        unique = trackers.run_step.unique[step_index]
        @test !used_uniques[unique]
        used_uniques[unique] = true
    end
end


function check_d_foreach(; expected_used_processes::Int = nprocs(), flags...)::Nothing
    reset_test!()
    run_foreach(d_foreach; flags...)
    check_did_all_steps()
    check_used_single_thread_of_processes(expected_used_processes)
    check_different_unique()
    return nothing
end

@testset "d_foreach" begin
    @testset "default" begin
        check_d_foreach()
    end

    @testset "batch_factor" begin
        @testset "one" begin
            check_d_foreach(batch_factor = 1)
        end
        @testset "many" begin
            check_d_foreach(batch_factor = typemax(Int))
        end
    end

    @testset "minimal_batch" begin
        @testset "half" begin
            check_d_foreach(
                expected_used_processes = 2,
                minimal_batch = div(steps_count, 2),
            )
        end
        @testset "all" begin
            check_d_foreach(expected_used_processes = 1, minimal_batch = typemax(Int))
        end
    end
end
