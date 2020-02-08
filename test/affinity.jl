using Base.Threads
using Random

MAX_DEPTH = 8

depth_switched = zeros(Bool, MAX_DEPTH)

used_threads = zeros(Bool, nthreads())

function sticky(body)
    thread_id = threadid()
    used_threads[thread_id] = true

    body()

    # If this fails, then the ParallelResource per-thread resources will not be safe.
    @test thread_id == threadid()

    return nothing
end

function recurse(thread_id, depth, rng)
    if thread_id != threadid()
        depth_switched[depth] = true
    end

    if depth == MAX_DEPTH
        sticky() do
            sleep(rand(rng, 1)[1] / 10)
        end
        return nothing
    end

    seed = rand(rng, Int, 1)[1]
    while seed < 1
        seed = rand(rng, Int, 1)[1]
    end

    result = nothing

    sticky() do
        thread_id = threadid()
        result = Threads.@spawn recurse(thread_id, depth + 1, MersenneTwister(seed))
    end

    recurse(threadid(), depth + 1, rng)

    sticky() do
        wait(result)
    end
end

function affinity() end

@testset "affinity" begin
    @test nthreads() > 1

    recurse(threadid(), 1, MersenneTwister(123456))

    for thread_id = 1:nthreads()
        @test used_threads[thread_id]
    end

    @test !depth_switched[1]
    @test any(depth_switched[2:MAX_DEPTH])
end
