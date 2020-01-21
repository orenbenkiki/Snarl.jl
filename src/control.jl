"""
Provide parallel control flow primitives.
"""
module Control

using Base.Threads
using Distributed
using ..Launched
using ..Resources

export SimdFlag
export default_batch_simd
export default_thread_batches
export everywhere
export t_foreach

"""
Run a function once on each process (including the main process).
"""
function everywhere(body::Function)::Nothing
    @sync begin
        for worker in workers()
            @spawnat worker body()
        end
        body()
    end

    return nothing
end

"""
The `@simd` directive to use for an inner loop.
"""
const SimdFlag = Union{Bool,Symbol,Val{true},Val{false},Val{:ivdep}}

"""
The default `@simd` directive to apply to the inner loops.

This is `:ivdep` because the code here assumes all steps are entirely independent. Any coordination
is expected to be done using the appropriate `ParallelResources`.
"""
const default_batch_simd = :ivdep

"""
The default number of batches to run in each thread.

Scheduling is done in equal-size batches where on average each thread will execute `thread_batches`
such batches. Setting this to a value large than one compensates for variability between computation
time of such batches. However setting this to higher values also increases scheduling overhead. The
default is `4` which is assumed to be a reasonable compromise.
"""
const default_thread_batches = 4

function simd_foreach(
    step::Function,
    resources::ParallelResources,
    values,
    simd::SimdFlag,
)::Nothing
    if simd == :ivdep
        @simd ivdep for value in values
            step(resources, value)
        end

    elseif simd == true
        @simd for value in values
            step(resources, value)
        end

    elseif simd == false
        for value in values
            step(resources, value)
        end

    else
        throw(ArgumentError("invalid simd flag: $(simd)"))
    end

    return nothing
end

function run_all(
    step::Function,
    resources::ParallelResources,
    values,
    simd::SimdFlag,
)::Nothing
    simd_foreach(resources, values, simd) do resources, value
        return step(resources, value)
    end

    return nothing
end

function run_batch(
    step::Function,
    resources::ParallelResources,
    values,
    batch_index::Int,
    batch_size::Number,
    simd::SimdFlag,
)::Nothing
    first_step_index = round(Int, (batch_index - 1) * batch_size) + 1
    last_step_index = round(Int, batch_index * batch_size)
    @assert 1 <= first_step_index &&
            first_step_index <= last_step_index && last_step_index <= length(values)

    simd_foreach(resources, first_step_index:last_step_index, simd) do resources, step_index
        return step(resources, @inbounds values[step_index])
    end
end

"""
    t_foreach(step::Function, resources::ParallelResources, values::collection;
              thread_batches=default_thread_batches, simd=default_batch_simd)

Perform a step for each value in the collection in parallel using multiple threads in the current
process. This uses `@threads` internally.

Scheduling is done in equal-size batches where on average each thread will execute `thread_batches`
such batches. Setting this to a value large than one compensates for variability between computation
time of such batches. However setting this to higher values also increases scheduling overhead.

Each batch is executed as an inner loop using the specified `batch_simd`.

Each batch will contain at least `minimal_batch` iterations, even if this means using a smaller
number of threads.
"""
function t_foreach(
    step::Function,
    resources::ParallelResources,
    values;
    thread_batches::Int = default_thread_batches,
    minimal_batch::Int = 1,
    batch_simd::SimdFlag = default_batch_simd,
)::Nothing
    @assert nthreads() > 1
    @assert thread_batches > 0
    @assert minimal_batch > 0

    if length(values) <= minimal_batch
        run_all(step, resources, values, batch_simd)
        return nothing
    end

    batches_count = nthreads() * thread_batches
    batch_size = length(values) / batches_count
    if batch_size < minimal_batch
        batch_size = minimal_batch
        batches_count = round(Int, length(values) / minimal_batch)
    end

    if batches_count <= nthreads()
        @threads for batch_index = 1:batches_count
            run_batch(step, resources, values, batch_index, batch_size, batch_simd)
        end
    else
        next_batch_index = Atomic{Int}(1)
        @threads for thread_index = 1:nthreads()
            while true
                batch_index = atomic_add!(next_batch_index, 1)
                batch_index > batches_count && break
                run_batch(step, resources, values, batch_index, batch_size, batch_simd)
            end
        end
    end

    return nothing
end

end # module
