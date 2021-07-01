# Running tasks

Having initialized the application as described above, we can finally perform the actual
computations required by the application.

`Snarl` does not try to hide the differences between threads and processes. Instead it provides
explicit functions to leverage both in a controlled way, using:

```@docs
Snarl.Control
```

`Snarl` is built around the common construct of the parallel loop; that is, perform some computation
for each element of some collection (commonly known as the SPMD - Single Program Multiple Data -
programming model). Thus the basic constructs provided are forms of the `foreach` loop.

## Serial code

Normally serial code requires no special API. However, it is useful to be able to easily convert a
parallel piece of code to serial and back, e.g. for debugging. Therefore `Snarl` provides a serial
`foreach` loop:

```@docs
Snarl.Control.s_foreach
```

The type of the `simd` flag is:

```@docs
Snarl.Control.SimdFlag
```

The default is controlled by:

```@docs
Snarl.Control.default_simd
```

## Using threads

Running steps on threads of the same process allows them to freely share data, which makes for very
efficient code, at the cost of restricting the processing to the current process (physical machine).
To achieve this, invoke:

```@docs
Snarl.Control.t_foreach
```

This introduces the concept of a `batch_factor` and `minimal_batch` which can be manually adjusted
to achieve the best performance. Their defaults are controlled by:

```@docs
Snarl.Control.default_batch_factor
Snarl.Control.default_minimal_batch
```

## Using processes

Running steps on different processors (physical machines), serially in each one, allows for
efficiently leveraging per-machine resources such as RAM, disk, network, etc. If no worker processes
were launched, then this just runs the loop serially on the current (only) machine. To achieve
this, invoke:

```@docs
Snarl.Control.d_foreach
```

Note that as the return value of each `step` is discarded, you would need to set up some mechanism
to preserve the results of the processing. Since the worker processes are expected to run on
different machines, forms of shared memory will not work, but channels would. In addition, if the
machines have a shared persistent storage, such as a networked file system or database, then the
steps may deposit their results in there. This is actually a common use case for using this function
as a single machine may be physically restricted in the amount of traffic it can generate to the
shared storage, while multiple machines may be able to generate even more, without overwhelming the
bandwidth or compute resources of the shared storage, thereby reducing the overall processing time.

## Using both processes and threads

For ultimate scalability, we can run the steps using all the threads of all the processes. To
achieve this, invoke:

```@docs
Snarl.Control.dt_foreach
```

Here we can use shared memory between the threads in the same process, but still need to set up some
mechanism for preserving the results of the steps since they execute on separate machines.

This introduces the concept of a `distribution` policy:

```@docs
Snarl.Control.DistributionPolicy
```

Whose default is controlled by:

```@docs
Snarl.Control.default_distribution
```

## Manually spawning tasks

`Snarl` also provides a mechanism allowing you to spawn a task on another process, trying to
load-balance the tasks between all processes:

```@docs
Snarl.Control.next_worker!()
```

This is intended to be used as `@spawnat next_worker!() ...`. However, keep in mind that (at least
in the current version) this uses a simplistic round-robin method to spread the tasks between
processes. This is sub-optimal but is assumed to be acceptable when spreading around many tasks
(such as when using `foreach` loops). When manually invoking very few tasks it may overload some
machines while letting others go idle. A proper solution would be to introduce a true global
scheduler for distributed processes, which is a daunting task.
