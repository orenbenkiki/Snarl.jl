"""
Storage of values for parallel algorithms.
"""
module Storage

using Base.Threads
import Base.Semaphore

import Distributed: clear!

export GlobalStorage, LocalStorage, ParallelStorage

export add_per_process!, add_per_step!, add_per_thread!
export get_per_process, get_per_step, get_per_thread, get_semaphore, get_value
export clear_per_process!, clear_per_thread!, clear_per_step!, clear!
export forget_per_process!, forget_per_thread!, forget_per_step!, forget!
export with_per_process, with_value

"""
    GlobalStorage(; [make::Function=nothing, clear::Function=nothing, value::Any=nothing])

Provide storage for a per-process ("global") value for a parallel algorithm.

One instance of the value will be created per process. All process threads will access the same
instance, so either access would be read-only, thread-safe mutation, or the threads will need to
coordinate their access in some way.

The `make` function is used to lazily construct the initial value on each process. If an initial
`value` is provided, there is no need to provide a `make` function. Note that such a value would be
transmitted through the network to any worker process(es) using the value. For large values, it is
often preferable to re-construct them on each process using the `make` function.

The `clear` function is used to release associated non-memory resources when the value is no longer
needed, e.g. closing files. It is invoked with the value to clear.
"""
mutable struct GlobalStorage
    make::Union{Type,Function,Nothing}

    clear::Union{Function,Nothing}

    value::Any

    semaphore::Semaphore

    function GlobalStorage(;
        make::Union{Type,Function,Nothing} = nothing,
        clear::Union{Function,Nothing} = nothing,
        value = nothing,
    )::GlobalStorage
        if value == nothing
            @assert make != nothing "GlobalStorage is missing both a make function and an initial value"
        else
            @assert make == nothing "GlobalStorage specifies both a make function and an initial value"  # untested
        end
        return new(make, clear, value, Semaphore(1))
    end
end

"""
    get_value(storage::GlobalStorage)::Any

Get the value of a global storage.

This will create the value if necessary using the registered `make` function.
"""
function get_value(storage::GlobalStorage)::Any
    if storage.value == nothing
        @assert storage.make != nothing "Getting missing GlobalStorage without a make function"
        Base.acquire(storage.semaphore)
        try
            if storage.value == nothing
                storage.value = storage.make()
            end
        finally
            Base.release(storage.semaphore)
        end
    end
    return storage.value
end

"""
    with_value(body::Function, storage::GlobalStorage)::Any

Invoke and return the results of the `body` function, which is given exclusive access to the
per-process value, by using its semaphore. This assumes no other thread is using `get_value` to get
direct access to the value in parallel.

This will create the value if necessary using the registered `make` function.
"""
function with_value(body::Function, storage::GlobalStorage)::Any
    Base.acquire(storage.semaphore)  # untested
    try
        if storage.value == nothing  # untested
            @assert storage.make != nothing "Getting missing GlobalStorage without a make function"  # untested
            storage.value = storage.make()  # untested
        end
        return body(storage.value)  # untested
    finally
        Base.release(storage.semaphore)  # untested
    end
end

"""
Clear the value of a global storage.

At minimum, this will allow the value to be garbage collected. If a `clear` function was registered,
it can release associated non-memory resources, e.g. closing files.

This is never invoked implicitly. It is only done if explicitly invoked. That is, this is not a
"destructor".
"""
function clear!(storage::GlobalStorage)::Nothing
    value = storage.value
    if value != nothing
        Base.acquire(storage.semaphore)
        try
            value = storage.value
            storage.value = nothing
        finally
            Base.release(storage.semaphore)
        end
    end

    if value != nothing && storage.clear != nothing
        storage.clear(value)  # untested
    end

    return nothing
end

"""
    LocalStorage(make::Function; [clear::Function=nothing, reset::Function=nothing])

Provide storage for a per-thread ("local") value for a parallel algorithm.

One instance of the value will be created per thread of the process. This value instance will be
constructed lazily when each thread of the process requests the value. As each thread will get its
own instance, it can freely modify it without the need to coordinate with other threads.

The `make` function is mandatory as we'll need to lazily create a separate new value instance per
thread.

The `clear` function is used to release associated non-memory resources when the value is no longer
needed, e.g. closing files. It is invoked with the value to clear.

The `reset` function allows isolating the value between steps. When a step accesses the value, it is
automatically reset (e.g., arrays may be filled with zeros) so that the step can use it without the
risk of contamination from the previous step executing on the same thread. It is assumed that
`reset` is cheaper than garbage-collecting the data used by the previous step and re-allocating and
initializing a fresh instance for the next step.
"""
mutable struct LocalStorage
    make::Union{Type,Function}

    reset::Union{Function,Nothing}

    clear::Union{Function,Nothing}

    values::Array{Any,1}

    LocalStorage(;
        make::Union{Type,Function},
        clear::Union{Function,Nothing} = nothing,
        reset::Union{Function,Nothing} = nothing,
    ) = new(make, reset, clear, Array{Any,1}(nothing, nthreads()))
end

"""
    get_value(storage::LocalStorage, [thread_id::Int=threadid()])::Any

Get the value of a local storage for the specified thread (by default, the current thread).
Accessing the data of another thread requires it be immutable, allow for thread-safe mutability, or
be coordinated in some way.

This will create the value if necessary using the registered `make` function.
"""
function get_value(storage::LocalStorage, thread_id::Int = threadid())::Any
    value = @inbounds storage.values[thread_id]
    if value == nothing
        @assert storage.make != nothing "Getting missing LocalStorage without a make function"
        value = storage.make()
        @inbounds storage.values[thread_id] = value
    end
    if storage.reset != nothing
        storage.reset(value)
    end
    return value
end

"""
    clear!(storage::LocalStorage, [thread_id=0])

Clear all or one of the values of a local storage. If a thread is specified, clear only the value
for that thread. If a zero thread is specified, clear all the values of all the threads. If a
negative thread is specified, clear all the other threads but keep the value for this one.

At minimum, this will allow the value(s) to be garbage collected. If a `clear` function was
registered, it can release associated non-memory resources, e.g. closing files.

This is never invoked implicitly. It is only done if explicitly invoked. That is, this
is not a "destructor".

Clearing the data of other threads requires coordination between the threads. This is mainly
provided to allow cleanup after all threads are completed.
"""
function clear!(storage::LocalStorage, thread_id::Int = 0)::Nothing
    @assert -nthreads() <= thread_id && thread_id <= nthreads()

    if thread_id == 0
        if storage.clear != nothing
            for value in storage.values  # untested
                if value != nothing  # untested
                    storage.clear(value)  # untested
                end
            end
        end
        storage.values[:] .= nothing

    elseif thread_id > 0  # untested
        value = storage.values[thread_id]  # untested
        @inbounds storage.values[thread_id] = nothing  # untested
        if value != nothing && storage.clear != nothing  # untested
            storage.clear(value)  # untested
        end
    else
        for clear_thread_id = 1:nthreads()  # untested
            clear_thread_id != -thread_id || continue  # untested
            @inbounds value = storage.values[clear_thread_id]  # untested
            @inbounds storage.values[clear_thread_id] = nothing  # untested
            if value != nothing && storage.clear != nothing  # untested
                storage.clear(value)  # untested
            end
        end
    end

    return nothing
end

"""
Provide storage of values for a parallel algorithm.

This provides a generic storage which allows accessing each value by its scope and name. Values are
created lazily, when threads actually request them. This allows the value creation to be parallel.
"""
mutable struct ParallelStorage
    """
    A generic container for the per-process (global) stored values.

    Per-process values are shared between all the threads of the process, so access would be either
    read-only, allow thread-safe mutation, or the threads will need to coordinate their access in
    some way.
    """
    per_process::Dict{String,GlobalStorage}

    """
    A generic container for the per-thread (local) stored values.

    Per-thread values maintain their state between steps executing on the same thread. This makes
    them useful to implement reductions when the result data is large.
    """
    per_thread::Dict{String,LocalStorage}

    """
    A generic container for the per-step (local) stored values.

    Per-step values are reset at the start of each step. This allows allocating them once per thread
    and reusing them in each step without worrying about contamination from the previous steps which
    executed on the same thread.
    """
    per_step::Dict{String,LocalStorage}

    ParallelStorage() = new(Dict{String,Any}(), Dict{String,Any}(), Dict{String,Any}())
end

"""
    add_per_process!(storage::ParallelStorage, name::String;
                     [make::Function, clear::Function, value::Any])

Add a new per-process value to the storage under the specified `name`.

Specify either a `make` function xor an initial `value`. Note that such a value would be transmitted
through the network to any worker process(es) using the value. For large values, it is often
preferable to re-construct them on each process using the `make` function.
"""
function add_per_process!(
    storage::ParallelStorage,
    name::String;
    make::Union{Type,Function,Nothing} = nothing,
    clear::Union{Function,Nothing} = nothing,
    value = nothing,
)::Nothing
    storage.per_process[name] = GlobalStorage(make = make, clear = clear, value = value)
    return nothing
end

"""
    add_per_thread!(storage::ParallelStorage, name::String, make=Function; [clear=Function])

Add a new per-process value to the storage under the specified `name`.
"""
function add_per_thread!(
    storage::ParallelStorage,
    name::String;
    make::Union{Type,Function},
    clear::Union{Function,Nothing} = nothing,
)::Nothing
    storage.per_thread[name] = LocalStorage(make = make, clear = clear)
    return nothing
end

"""
    add_per_step!(storage::ParallelStorage, name::String, make::Function, reset::Function;
                  [clear::Function])

Add a new per-step value to the storage under the specified `name`.
"""
function add_per_step!(
    storage::ParallelStorage,
    name::String;
    make::Union{Type,Function},
    reset::Function,
    clear::Union{Function,Nothing} = nothing,
)::Nothing
    storage.per_step[name] = LocalStorage(make = make, reset = reset, clear = clear)
    return nothing
end

"""
    get_semaphore(storage::ParallelStorage, name::String)

Obtain a semaphore for coordinating access to a per-process value value between all threads of
the process.
"""
function get_semaphore(storage::ParallelStorage, name::String)::Semaphore
    return storage.per_process[name].semaphore  # untested
end

"""
    get_per_process(storage::ParallelStorage, name::String)

Obtain the value of a per-process value from the storage by its name.

The value will be created if necessary (once per process). It will be shared by all the threads of
the process, so access would be either read-only, allow thread-safe mutation, or the threads will
need to coordinate their access in some way, for example using `with_per_process`.

This will create the value if necessary using the registered `make` function.
"""
function get_per_process(storage::ParallelStorage, name::String)::Any
    return get_value(storage.per_process[name])
end

"""
    with_per_process(body::Function, storage::ParallelStorage, name::String)

Invoke and return the results of the `body` function, which is given exclusive access to the
per-process value, by using its semaphore.

This assumes no other thread is using `get_per_process` to get direct access to the value in
parallel.

This will create the value if necessary using the registered `make` function.
"""
function with_per_process(body::Function, storage::ParallelStorage, name::String)::Any
    return with_value(body, storage.per_process[name])  # untested
end

"""
    get_per_thread(storage::ParallelStorage, name::String, thread_id=threadid())

Obtain the value of a per-thread value from the storage by its name, for the specified thread (by
default, the current one).

The value will be created if necessary (once per thread). If previous steps executed in the thread,
then their modifications to the value will be visible in following steps.

This will create the value if necessary using the registered `make` function.
"""
function get_per_thread(
    storage::ParallelStorage,
    name::String,
    thread_id::Int = threadid(),
)::Any
    return get_value(storage.per_thread[name], thread_id)
end

"""
    get_per_step(storage::ParallelStorage, name::String)

Obtain the value of a per-step value from the storage by its name, for the specified thread (by
default, the current one). The value will be created if necessary (once per thread), and will be
reset every time it is fetched, so that modifications by previous steps in the same thread will not
be visible in following steps.

This will create the value if necessary using the registered `make` function.
"""
function get_per_step(
    storage::ParallelStorage,
    name::String,
    thread_id::Int = threadid(),
)::Any
    return get_value(storage.per_step[name], thread_id)
end

"""
    clear_per_process!(storage::LocalStorage, name::String)

Clear the value of a per-process value from the storage by its name.

At minimum, this will allow the value to be garbage collected. If a `clear` function was
registered, it can release associated non-memory resources, e.g. closing files.

This is never invoked implicitly. It is only done if explicitly invoked. That is, this
is not a "destructor".
"""
function clear_per_process!(storage::ParallelStorage, name::String)::Nothing
    clear!(storage.per_process[name])  # untested
    return nothing  # untested
end

"""
    clear_per_thread!(storage::LocalStorage, name::String, [thread_id::Int])

Clear all or one of the values of a per-thread storage. If a thread is specified, clear only the
value for that thread. If a negative thread is specified, clear all the other threads but keep the
value for this one.

At minimum, this will allow the value(s) to be garbage collected. If a `clear` function was
registered, it can release associated non-memory resources, e.g. closing files.

This is never invoked implicitly. It is only done if explicitly invoked. That is, this
is not a "destructor".
"""
function clear_per_thread!(storage::ParallelStorage, name::String, thread_id = 0)::Nothing
    clear!(storage.per_thread[name], thread_id)  # untested
    return nothing  # untested
end

"""
    clear_per_step!(storage::LocalStorage, name::String, [thread_id::Int])

Clear all or one of the values of a per-step storage. If a thread is specified, clear only the value
for that thread. If a negative thread is specified, clear all the other threads but keep the value
for this one.

At minimum, this will allow the value(s) to be garbage collected. If a `clear` function was
registered, it can release associated non-memory resources, e.g. closing files.

This is never invoked implicitly. It is only done if explicitly invoked. That is, this
is not a "destructor".
"""
function clear_per_step!(storage::ParallelStorage, name::String, thread_id = 0)::Nothing
    clear!(storage.per_step[name], thread_id)  # untested
    return nothing  # untested
end

"""
    clear!(storage::ParallelStorage)

Clear all the values.

This releases all the values to the garbage collector, and also invokes the registered `clear`
functions to release any associated non-memory resources, e.g. closing files.

This is never invoked implicitly. It is only done if explicitly invoked. That is, this
is not a "destructor".
"""
function clear!(storage::ParallelStorage)::Nothing
    for global_storage in values(storage.per_process)
        clear!(global_storage)
    end

    for local_storage in values(storage.per_thread)
        clear!(local_storage)
    end

    for local_storage in values(storage.per_step)
        clear!(local_storage)
    end

    return nothing
end

"""
    forget_per_process!(storage::LocalStorage, name::String)

Clear and completely forget the value of a per-process value from the storage by its name.
"""
function forget_per_process!(storage::ParallelStorage, name::String)::Nothing
    clear!(storage.per_process[name])  # untested
    delete!(storage.per_process, name)  # untested
    return nothing  # untested
end

"""
    forget_per_thread!(storage::LocalStorage, name::String)

Clear all of the values of a per-thread storage and completely forget it.
"""
function forget_per_thread!(storage::ParallelStorage, name::String)::Nothing
    clear!(storage.per_thread[name])  # untested
    delete!(storage.per_thread, name)  # untested
    return nothing  # untested
end

"""
    forget_per_step!(storage::LocalStorage, name::String)

Clear all of the values of a per-step storage and completely forget it.
"""
function forget_per_step!(storage::ParallelStorage, name::String)::Nothing
    clear!(storage.per_step[name])  # untested
    delete!(storage.per_step, name)  # untested
    return nothing  # untested
end

"""
    forget!(storage::ParallelStorage)

Clear and forget all the values.
"""
function forget!(storage::ParallelStorage)::Nothing
    clear!(storage)

    storage.per_process = Dict{String,Any}()
    storage.per_thread = Dict{String,Any}()
    storage.per_step = Dict{String,Any}()

    return nothing
end

end # module
