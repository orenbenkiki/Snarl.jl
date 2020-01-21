"""
Manage resource values for parallel algorithms.
"""
module Resources

import Base.Semaphore

export GlobalResource
export LocalResource
export ParallelResources

export add_per_process!
export add_per_step!
export add_per_thread!
export clear!
export get_per_process
export get_per_step
export get_per_thread
export get_semaphore
export get_value
export with_per_process
export with_value

"""
Provide a per-process ("global") value to a parallel algorithm.

One instance of the resource value will be created per process. Typically, this value will be
constructed lazily when the first thread of the process requests the value. All threads will
access the same instance, so either access would be read-only, thread-safe mutation, or the
threads will need to coordinate their access in some way.
"""
mutable struct GlobalResource
    """
    How to create a new value.

    This is only needed if a value is not provided on construction.

    This is invoked without any arguments.
    """
    make::Union{Type,Function,Nothing}

    """
    Cleanup when the value is no longer needed.

    This allows releasing resources, such as closing files.

    This is invoked with the resource to clear.
    """
    clear::Union{Function,Nothing}

    """
    The resource value.

    This may be `nothing` if a value is not provided on construction and the value was
    not requested yet.
    """
    value::Any

    """
    Coordinate between the multiple threads in the current process.
    """
    semaphore::Semaphore

    """
        GlobalResource(; [make::Function, clear::Function, value::Any])

    Construct a new global resource.

    If an initial value is provided, there is no need to provide a `make` function. Note that such a
    value would be transmitted through the network to any worker process(es) using the resource. For
    large values, it is often preferable to re-construct them on each process using the `make`
    function.
    """
    function GlobalResource(;
        make::Union{Type,Function,Nothing} = nothing,
        clear::Union{Function,Nothing} = nothing,
        value = nothing,
    )::GlobalResource
        if value == nothing
            @assert make != nothing "Resource is missing both a make function and an initial value"
        else
            @assert make == nothing "Resource specifies both a make function and an initial value"
        end
        return new(make, clear, value, Semaphore(1))
    end
end

"""
    get_value(resource::GlobalResource)::Any

Get the value of a global resource.

This will create the value if necessary using the registered `make` function.
"""
function get_value(resource::GlobalResource)::Any
    if resource.value == nothing
        @assert resource.make != nothing "Getting missing resource without a make function"
        Base.acquire(resource.semaphore)
        try
            if resource.value == nothing
                resource.value = resource.make()
            end
        finally
            Base.release(resource.semaphore)
        end
    end
    return resource.value
end

"""
    with_value(body::Function, resource::GlobalResource)::Any

Invoke and return the results of the `body` function, which is given exclusive access to the
per-process resource value, by using its semaphore. This assumes no other thread is using
`get_value` to get direct access to the resource value in parallel.

This will create the value if necessary using the registered `make` function.
"""
function with_value(body::Function, resource::GlobalResource)::Any
    Base.acquire(resource.semaphore)
    try
        if resource.value == nothing
            @assert resource.make != nothing "Getting missing resource without a make function"
            resource.value = resource.make()
        end
        return body(resource.value)
    finally
        Base.release(resource.semaphore)
    end
end

"""
Clear the value of a global resource.

At minimum, this will allow the value top be garbage collected.
If a `clear` function was registered, it can perform additional cleanup (such as closing files).

This is never invoked implicitly. It is only done if explicitly invoked. That is, this
is not a "destructor".
"""
function clear!(resource::GlobalResource)::Nothing
    if resource.value != nothing
        Base.acquire(resource.semaphore)
        try
            if resource.value != nothing
                if resource.clear != nothing
                    resource.clear(resource.value)
                end
                resource.value = nothing
            end
        finally
            Base.release(resource.semaphore)
        end
    end
    return nothing
end

"""
Provide a per-thread ("local") value to a parallel algorithm.

One instance of the resource will be created per thread of the process. This value instance will be
constructed lazily when each thread of the process requests the value. As each thread will get its
own instance, it can freely modify it without the need to coordinate with other threads.
"""
mutable struct LocalResource
    """
    How to create a new value.

    This is invoked without any arguments.
    """
    make::Union{Type,Function}

    """
    How to reset the value for each step.

    This allows allocating per-step value only once per thread. When each step accesses the value,
    it is automatically reset (e.g., arrays may be filled with zeros) so that the step can use it
    without the risk of contamination from the previous step executing on the same thread.

    This is invoked with the resource to reset.
    """
    reset::Union{Function,Nothing}

    """
    Cleanup when the value is no longer needed.

    This allows releasing resources, such as closing files.

    This is invoked with the resource to clear.
    """
    clear::Union{Function,Nothing}

    """
    The per-thread resource values.

    Initially all values are `nothing`. When each thread first accesses the value,
    it is created using the `make` function.
    """
    values::Array{Any,1}

    """
        LocalResource(make::Function; [reset::Function, clear::Function])

    Construct a new local resource. The `make` function is mandatory as we'll need to lazily create
    a new value instance per thread.
    """
    LocalResource(;
        make::Union{Type,Function},
        clear::Union{Function,Nothing} = nothing,
        reset::Union{Function,Nothing} = nothing,
    ) = new(make, reset, clear, Array{Any,1}(nothing, Threads.nthreads()))
end

"""
    get_value(resource::LocalResource)::Any

Get the value of a local resource.

This will create the value if necessary using the registered `make` function.
"""
function get_value(resource::LocalResource)::Any
    thread_id = Threads.threadid()
    value = resource.values[thread_id]
    if value == nothing
        @assert resource.make != nothing "Getting missing resource without a make function"
        value = resource.make()
        resource.values[thread_id] = value
    end
    if resource.reset != nothing
        resource.reset(value)
    end
    return value
end

"""
Clear the value of a local resource.

At minimum, this will allow the value top be garbage collected.
If a `clear` function was registered, it can perform additional cleanup (such as closing files).

This is never invoked implicitly. It is only done if explicitly invoked. That is, this
is not a "destructor".
"""
function clear!(resource::LocalResource)::Nothing
    if resource.clear != nothing
        for value in resource.values
            if value != nothing
                resource.clear(value)
            end
        end
    end
    resource.values[:] = nothing
    return nothing
end

"""
Provide values to a parallel algorithm.

This provides a generic container which allows accessing each resource by its scope and name.
Resource values are created lazily, when threads actually request them. This allows the
value creation to be parallel.
"""
struct ParallelResources
    """
    A generic container for the per-process (global) resources.

    Per-process resource values are shared between all the threads of the process, so either access
    would be read-only, thread-safe mutation, or the threads will need to coordinate their access in
    some way.
    """
    per_process::Dict{String,GlobalResource}

    """
    A generic container for the per-thread (local) resources.

    Per-thread resource values maintain their state between steps executing on the same thread. This
    makes them useful to implement reductions when the result data is large.
    """
    per_thread::Dict{String,LocalResource}

    """
    A generic container for the per-step (local) resources.

    Per-step resource values are reset at the start of each step. This allows allocating them once
    per thread and reusing them in each step without worrying about contamination from the previous
    steps which executed on the same thread.
    """
    per_step::Dict{String,LocalResource}

    """
        ParallelResources()

    Construct a new empty generic parallel resources container.
    """
    ParallelResources() = new(Dict{String,Any}(), Dict{String,Any}(), Dict{String,Any}())
end

"""
    add_per_process!(resources::ParallelResources, name::String; [make::Function, clear::Function, value::Any])

Add a new per-process resource to the container under the specified `name`.

Specify either a `make` function xor an initial `value`. Note that such a value would be transmitted
through the network to any worker process(es) using the resource. For large values, it is often
preferable to re-construct them on each process using the `make` function.
"""
function add_per_process!(
    resources::ParallelResources,
    name::String;
    make::Union{Type,Function,Nothing} = nothing,
    clear::Union{Function,Nothing} = nothing,
    value = nothing,
)::Nothing
    resources.per_process[name] = GlobalResource(make = make, clear = clear, value = value)
    return nothing
end

"""
    add_per_thread!(resources::ParallelResources, name::String, make=Function; [clear=Function])

Add a new per-process resource to the container under the specified `name`.
"""
function add_per_thread!(
    resources::ParallelResources,
    name::String;
    make::Union{Type,Function},
    clear::Union{Function,Nothing} = nothing,
)::Nothing
    resources.per_thread[name] = LocalResource(make = make, clear = clear)
    return nothing
end

"""
    add_per_step!(resources::ParallelResources, name::String, make::Function, reset::Function; [clear::Function])

Add a new per-step resource to the container under the specified `name`.
"""
function add_per_step!(
    resources::ParallelResources,
    name::String;
    make::Union{Type,Function},
    reset::Function,
    clear::Union{Function,Nothing} = nothing,
)::Nothing
    resources.per_step[name] = LocalResource(make = make, reset = reset, clear = clear)
    return nothing
end

"""
    get_semaphore(resources::ParallelResources, name::String)

Obtain a semaphore for coordinating access to a per-process resource value between all threads of
the process.
"""
function get_semaphore(resources::ParallelResources, name::String)::Semaphore
    return resources.per_process[name].semaphore
end

"""
    get_per_process(resources::ParallelResources, name::String)

Obtain the value of a per-process resource from the container by its name. The value will be created
if necessary (once per process). It will be shared by all the threads of the process, so either
access would be read-only, thread-safe mutation, or the threads will need to coordinate their access
in some way, for example using `with_per_process`.

This will create the value if necessary using the registered `make` function.
"""
function get_per_process(resources::ParallelResources, name::String)::Any
    return get_value(resources.per_process[name])
end

"""
    with_per_process(body::Function, resources::ParallelResources, name::String)

Invoke and return the results of the `body` function, which is given exclusive access to the
per-process resource value, by using its semaphore. This assumes no other thread is using
`get_per_process` to get direct access to the resource value in parallel.

This will create the value if necessary using the registered `make` function.
"""
function with_per_process(body::Function, resources::ParallelResources, name::String)::Any
    return with_value(body, resources.per_process[name])
end

"""
    get_per_thread(resources::ParallelResources, name::String)

Obtain the value of a per-thread resource from the container by its name. The value will be created
if necessary (once per thread). If previous steps executed in the thread, then their modifications
to the value will be visible in following steps.

This will create the value if necessary using the registered `make` function.
"""
function get_per_thread(resources::ParallelResources, name::String)::Any
    return get_value(resources.per_thread[name])
end

"""
    get_per_step(resources::ParallelResources, name::String)

Obtain the value of a per-step resource from the container by its name. The value will be created
if necessary (once per thread), and will be reset every time it is fetched, so that modifications
by previous steps in the same thread will not be visible in following steps.

This will create the value if necessary using the registered `make` function.
"""
function get_per_step(resources::ParallelResources, name::String)::Any
    return get_value(resources.per_step[name])
end

"""
    clear!(resources::ParallelResources)

Clear and forget all the resources.

This releases all the resource values to the garbage collector, and also releases any associated
non-memory resources such as closing files.

This also forgets the list of resources, completely clearing the container.

This is never invoked implicitly. It is only done if explicitly invoked. That is, this
is not a "destructor".
"""
function clear!(resources::ParallelResources)::Nothing
    clear!(resources.per_process)
    clear!(resources.per_thread)
    clear!(resources.per_step)
    resources.per_process = Dict{String,Any}()
    resources.per_thread = Dict{String,Any}()
    resources.per_step = Dict{String,Any}()
    return nothing
end

end # module
