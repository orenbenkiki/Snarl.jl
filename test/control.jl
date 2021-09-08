using Distributed

@everywhere using Base.Threads
@everywhere using SharedArrays
@everywhere using Snarl.Control
@everywhere using Snarl.Launched
@everywhere using Snarl.Storage
@everywhere using Snarl.DistributedChannels

@everywhere import Snarl.Storage: clear!

@info "Configure workers..."

const steps_count = 100

@everywhere const UNIQUE = 1
@everywhere const ACCUMULATOR = 2
@everywhere const MERGE = 3
const COUNTERS = 3

used_counters = zeros(Int, COUNTERS)

local_counters_channel =
    Channel{Tuple{Int,Union{Channel{Int},RemoteChannel{Channel{Int}}}}}(
        nprocs() * nthreads(),
    )
remote_counters_channel = RemoteChannel(() -> local_counters_channel)

@everywhere begin
    if myid() == 1
        counters_channel = $local_counters_channel
    else
        counters_channel = ThreadSafeRemoteChannel($remote_counters_channel)
    end
end

function serve_counters()::Nothing
    while true
        yield()
        request = take!(counters_channel)

        counter = request[1]
        response = request[2]

        used_counters[counter] += 1

        put!(response, used_counters[counter])
    end

    error("Never happens")  # untested
end

remote_do(serve_counters, 1)

@everywhere function next!(counter::Int)::Int
    response = request_response(request = counters_channel, response = Channel{Int}(1))
    put!(counters_channel, (counter, response))
    return take!(response)
end

function reset_counters!()::Nothing
    fill!(used_counters, 0)
    return nothing
end

@everywhere mutable struct OperationContext
    process::Int
    thread::Int
    unique::Int
    resets::Int
    OperationContext() = new(myid(), threadid(), next!(UNIQUE), 0)
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

function clear!(trackers::Trackers)::Nothing
    clear!(trackers.run_step)

    clear!(trackers.per_process)
    clear!(trackers.per_thread)
    clear!(trackers.per_step)

    fill!(trackers.per_step_resets, 0)

    return nothing
end

new_trackers = Trackers(
    new_context_trackers(),
    new_context_trackers(),
    new_context_trackers(),
    new_context_trackers(),
    new_tracking_array(),
)

@everywhere trackers = $new_trackers

function reset_trackers!()::Nothing
    clear!(trackers)
    return nothing
end

function reset_test!()::Nothing
    reset_counters!()
    reset_trackers!()
    return nothing
end

@everywhere function tracked_step(storage::ParallelStorage, step_index::Int)::Int
    @debug "step" step_index

    total_per_thread = get_per_thread(storage, "total")
    total_per_thread[1] += step_index

    track_context(trackers.run_step, step_index, OperationContext())

    per_step_context = get_per_step(storage, "context")
    @assert per_step_context.resets > 0
    trackers.per_step_resets[step_index] = per_step_context.resets

    track_context(trackers.per_step, step_index, per_step_context)
    track_context(trackers.per_process, step_index, get_per_process(storage, "context"))
    track_context(trackers.per_thread, step_index, get_per_thread(storage, "context"))

    return step_index
end

@everywhere function finalize_process(storage::ParallelStorage)::Nothing
    process_total = 0
    for thread_id = 1:nthreads()
        thread_total = get_per_thread(storage, "total", thread_id)[1]
        process_total += thread_total
    end
    results_channel = get_per_process(storage, "results_channel")
    put!(results_channel, process_total)
    return nothing
end

function check_same_values(values::AbstractArray, expected::Any)::Nothing
    for actual in values
        @test actual == expected
    end

    return nothing
end

function check_steps_did_run()::Nothing
    for step = 1:steps_count
        @test trackers.run_step.process[step] > 0
    end

    return nothing
end

function check_steps_used_single_process()::Nothing
    check_same_values(trackers.run_step.process, myid())

    check_same_values(trackers.per_process.process, myid())
    check_same_values(trackers.per_thread.process, myid())
    check_same_values(trackers.per_step.process, myid())

    return nothing
end

function check_steps_used_threads_of_single_process(expected_used_threads::Int)::Nothing
    used_threads = zeros(Bool, nthreads())

    check_steps_used_single_process()

    for step = 1:steps_count
        thread = trackers.run_step.thread[step]
        @test trackers.per_thread.thread[step] == thread
        @test trackers.per_step.thread[step] == thread
        used_threads[thread] = true
    end

    @test sum(used_threads) == expected_used_threads

    return nothing
end

function check_step_used_different_uniques()::Nothing
    used_uniques = zeros(Bool, used_counters[UNIQUE])
    for step_index = 1:steps_count
        unique = trackers.run_step.unique[step_index]
        @test !used_uniques[unique]
        used_uniques[unique] = true
    end
end

function foreach_storage()::ParallelStorage
    storage = ParallelStorage()
    add_per_process!(
        storage,
        "results_channel",
        value = Channel{Union{Int,Nothing}}(nprocs() + 1),
    )
    add_per_process!(storage, "context", make = OperationContext)
    add_per_thread!(storage, "context", make = OperationContext)
    add_per_step!(storage, "context", make = OperationContext, reset = increment_resets!)
    add_per_thread!(storage, "total", make = () -> Int[0])
    return storage
end

function check_results(results_channel::Channel{Union{Int,Nothing}})::Nothing
    put!(results_channel, nothing)
    close(results_channel)
    total::Int = 0
    while true
        result = take!(results_channel)
        if result == nothing
            break
        end
        total += result
    end
    @assert total == steps_count * (steps_count + 1) / 2
end

function run_foreach(foreach::Function; is_distributed::Bool, flags...)::Nothing
    storage = foreach_storage()
    foreach(tracked_step, storage, 1:steps_count; flags...)
    if !is_distributed
        finalize_process(storage)
    end
    results_channel = get_per_process(storage, "results_channel")
    check_results(results_channel)
    forget!(storage)
    return nothing
end

function check_s_foreach(; flags...)::Nothing
    @debug "BEGIN S_FOREACH TEST" flags
    reset_test!()
    run_foreach(s_foreach; is_distributed = false, flags...)
    check_steps_did_run()
    check_steps_used_threads_of_single_process(1)
    check_step_used_different_uniques()
    @debug "END S_FOREACH TEST"
    return nothing
end

@test_set "s_foreach/default" begin
    check_s_foreach()
end

@test_set "s_foreach/simd/invalid" begin
    @test_throws ArgumentError s_foreach(
        tracked_step,
        foreach_storage(),
        1:1,
        simd = :invalid,
    )
end
@test_set "s_foreach/simd/false" begin
    check_s_foreach(simd = false)
end
@test_set "s_foreach/simd/true" begin
    check_s_foreach(simd = true)
end
@test_set "s_foreach/simd/ivdep" begin
    check_s_foreach(simd = :ivdep)
end

function check_t_foreach(; expected_used_threads::Int = nthreads(), flags...)::Nothing
    @debug "BEGIN T_FOREACH TEST" expected_used_threads flags
    reset_test!()
    run_foreach(t_foreach; is_distributed = false, flags...)
    check_steps_did_run()
    check_steps_used_threads_of_single_process(expected_used_threads)
    check_step_used_different_uniques()
    @debug "END T_FOREACH TEST"
    return nothing
end

@test_set "t_foreach/default" begin
    check_t_foreach()
end

@test_set "t_foreach/batch_factor/one" begin
    check_t_foreach(batch_factor = 1)
end
@test_set "t_foreach/batch_factor/many" begin
    check_t_foreach(batch_factor = typemax(Int))
end

@test_set "t_foreach/minimal_batch/half" begin
    check_t_foreach(expected_used_threads = 2, minimal_batch = div(steps_count, 2))
end

@test_set "t_foreach/minimal_batch/all" begin
    check_t_foreach(expected_used_threads = 1, minimal_batch = typemax(Int))
end

function check_used_single_thread_of_processes(
    expected_used_processes::Int = nprocs(),
)::Nothing
    thread_of_processes = zeros(Int, nprocs())
    used_processes = 0

    for step = 1:steps_count
        process = trackers.run_step.process[step]
        thread = trackers.run_step.thread[step]
        @test trackers.per_thread.process[step] == process
        @test trackers.per_thread.thread[step] == thread
        @test trackers.per_step.process[step] == process
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

function check_step_used_different_uniques()::Nothing
    used_uniques = zeros(Bool, used_counters[UNIQUE])
    for step_index = 1:steps_count
        unique = trackers.run_step.unique[step_index]
        @test !used_uniques[unique]
        used_uniques[unique] = true
    end
end

function check_d_foreach(; expected_used_processes::Int = nprocs(), flags...)::Nothing
    @debug "BEGIN D_FOREACH TEST" expected_used_processes flags
    reset_test!()
    run_foreach(
        d_foreach;
        is_distributed = true,
        finalize_process = finalize_process,
        channel_names = "results_channel",
        flags...,
    )
    check_steps_did_run()
    check_used_single_thread_of_processes(expected_used_processes)
    check_step_used_different_uniques()
    @debug "END D_FOREACH TEST"
    return nothing
end

@test_set "d_foreach/default" begin
    check_d_foreach()
end

@test_set "d_foreach/batch_factor/one" begin
    check_d_foreach(batch_factor = 1)
end

@test_set "d_foreach/batch_factor/many" begin
    check_d_foreach(batch_factor = typemax(Int))
end

@test_set "d_foreach/minimal_batch/half" begin
    check_d_foreach(expected_used_processes = 2, minimal_batch = div(steps_count, 2))
end

@test_set "d_foreach/minimal_batch/all" begin
    check_d_foreach(expected_used_processes = 1, minimal_batch = typemax(Int))
end

function check_used_all_threads_of_processes(
    expected_used_processes::Int,
    expected_used_threads::Int,
)::Nothing
    used_processes = zeros(Bool, nprocs())
    used_threads_of_processes = zeros(Bool, (nprocs(), nthreads()))

    for step = 1:steps_count
        process = trackers.run_step.process[step]
        thread = trackers.run_step.thread[step]
        @test trackers.per_thread.process[step] == process
        @test trackers.per_thread.thread[step] == thread
        @test trackers.per_step.process[step] == process
        @test trackers.per_step.thread[step] == thread
        used_processes[process] = true
        used_threads_of_processes[process, thread] = true
    end

    @test sum(used_processes) == expected_used_processes
    @test sum(used_threads_of_processes) == expected_used_threads

    return nothing
end

function check_dt_foreach(;
    expected_used_processes::Int = nprocs(),
    expected_used_threads::Int = nprocs() * nthreads(),
    flags...,
)::Nothing
    @debug "BEGIN DT_FOREACH TEST" expected_used_processes expected_used_threads flags
    reset_test!()
    run_foreach(
        dt_foreach;
        is_distributed = true,
        finalize_process = finalize_process,
        channel_names = ["results_channel"],
        flags...,
    )
    check_steps_did_run()
    check_used_all_threads_of_processes(expected_used_processes, expected_used_threads)
    check_step_used_different_uniques()
    @debug "END DT_FOREACH TEST"
    return nothing
end

@test_set "dt_foreach/default" begin
    check_dt_foreach()
end

@test_set "dt_foreach/minimal_batch/one" begin
    check_dt_foreach(
        expected_used_processes = 1,
        expected_used_threads = 1,
        minimal_batch = typemax(Int),
    )
end

@test_set "dt_foreach/minimal_batch/many/maximize_processes" begin
    check_dt_foreach(
        expected_used_processes = nprocs(),
        expected_used_threads = nprocs(),
        minimal_batch = ceil(Int, steps_count / nprocs()),
        distribution = MaximizeProcesses,
    )
end

@test_set "dt_foreach/minimal_batch/many/minimize_processes" begin
    check_dt_foreach(
        expected_used_processes = 1,
        expected_used_threads = nthreads(),
        minimal_batch = ceil(Int, steps_count / nthreads()),
        distribution = MinimizeProcesses,
    )
end

@test_set "dt_foreach/minimal_batch/half/maximize_processes" begin
    check_dt_foreach(
        expected_used_processes = nprocs(),
        expected_used_threads = nprocs(),
        minimal_batch = ceil(Int, steps_count / nprocs()),
        distribution = MaximizeProcesses,
    )
end

@test_set "dt_foreach/minimal_batch/half/minimize_processes" begin
    minimal_batch = ceil(Int, steps_count / (2 * nthreads()))
    check_dt_foreach(
        expected_used_processes = 2,
        expected_used_threads = floor(Int, steps_count / minimal_batch),
        minimal_batch = minimal_batch,
        distribution = MinimizeProcesses,
    )
end
