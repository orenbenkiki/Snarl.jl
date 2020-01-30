using Base.Threads
using Random

SLEEP = 1
SPAWN = 2
WAIT = 3
MAX_REASON = 3

switched = zeros(Bool, MAX_REASON)

MAX_DEPTH = 8

depth_switched = zeros(Bool, MAX_DEPTH)

used_threads = zeros(Bool, nthreads())

function track(body, reason_index)
    thread_id = threadid()
    used_threads[thread_id] = true
    body()
    if thread_id != threadid()
        switched[reason_index] = true
    end
    return nothing
end

function recurse(thread_id, depth, rng)
    if thread_id != threadid()
        depth_switched[depth] = true
    end

    if depth == MAX_DEPTH
        track(SLEEP) do
            sleep(rand(rng, 1)[1] / 10)
        end
        return nothing
    end

    seed = rand(rng, Int, 1)[1]
    while seed < 1
        seed = rand(rng, Int, 1)[1]
    end

    result = nothing

    track(SPAWN) do
        thread_id = threadid()
        result = Threads.@spawn recurse(thread_id, depth + 1, MersenneTwister(seed))
    end

    recurse(threadid(), depth + 1, rng)

    track(WAIT) do
        wait(result)
    end
end

function affinity() end

@testset "affinity" begin
    @test nthreads() > 1

    recurse(threadid(), 1, MersenneTwister(123456))

    # If these fail, then the ParallelResource approach will need re-thinking.
    # Specifically, per_thread resources will not be safe.
    @test !switched[SLEEP]
    @test !switched[SPAWN]
    @test !switched[WAIT]

    for thread_id = 1:nthreads()
        @test used_threads[thread_id]
    end

    @test !depth_switched[1]
    for depth = 2:MAX_DEPTH
        @test depth_switched[depth]
    end
end
