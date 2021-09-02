using Distributed

@everywhere using Base.Threads
@everywhere using Snarl.Launched
@everywhere using Snarl.DistributedChannels
@everywhere using Snarl.DistributedLocks

@everywhere function test_seen_lock()::Bool
    seen_lock = false
    @sync @threads for _ = 1:nthreads()
        with_distributed_lock() do is_first
            if is_first
                @assert !seen_lock
                sleep(0.1)
            else
                @assert seen_lock
            end
            seen_lock = true
        end
    end
    return seen_lock
end

@test_set "locks/local" begin
    @test test_seen_lock()
end

@test_set "locks/remote" begin
    @test fetch(@spawnat 2 test_seen_lock())
end
