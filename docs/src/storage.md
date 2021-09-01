# Parallel storage

Running computation steps in parallel but raises issues of thread safety when accessing data,
especially since Julia lacks any language-level mechanisms for ensuring this (as opposed to Rust). A
second important concern is efficiency; if each step allocates and drops large intermediate data
structures (typically, arrays), the garbage collector will be needlessly stressed. It would be nice
to be able to reuse this intermediate data across steps, but we need to do this safely as we would
want to isolate the data used by each step from the other steps, even when running in the same
thread.

`Snarl` therefore provides the following:

```@docs
Snarl.Storage
```

## Global storage

When processing large data we often need "global" values. Ideally these are immutable, such as very
large input arrays, but might include large output arrays if different steps write into disjoint
slices of the data.

Since `Snarl` supports using different physical machines, it is not possible to use shared memory to
access the same instance of the data on the different processes. Instead we need to create an
independent instance of the "global" value for each process. This is merely tedious for immutable
(input) data, and requires some sort of handling for mutable (output) data, typically addressed by
using some shared resource across all machines, such as memory-mapping files on a common networked
file system, using connections to a shared networked database, etc.

Since obtaining the data may be expensive (e.g. reading in large amounts of data from a networked
file system), it makes sense to do so lazily. That is, we should only create an instance of the data
in a process if we actually run a step requiring this data in some thread of that process. This can
be a possible motivation for using the `:minimize_processes` `DistributionPolicy`.

`Snarl` encapsulates the handling of "global" per-process values by providing:

```@docs
Snarl.Storage.GlobalStorage
```

This provides the following operations:

### Cleanup

```@docs
Snarl.Storage.clear!(storage::Snarl.Storage.GlobalStorage)
```

### Access

Shared access (for immutable or thread-safe mutable data):

```@docs
Snarl.Storage.get_value(storage::Snarl.Storage.GlobalStorage)
```

And exclusive access:

```@docs
Snarl.Storage.with_value(body::Function, storage::Snarl.Storage.GlobalStorage)
```

## Local storage

Non-trivial processing steps often require a significant amount of local storage (typically,
arrays). It is easy to just allocate the array at the start of each step and let it be garbage
collected when the step is done, but this is inefficient.

Another type of local storage is an accumulator, where each step builds on the results of the
previous steps running in the same thread. This typically requires that once all steps are done, a
final serial loop will combine all the results from all the threads into a single value; if
distributing the work across processes, these per-process results all need to be sent to a single
process to be combined into the total result. Note that there would be no guarantee about the order
in which results are combined. For example, if the accumulator is a floating point value, which sums
the results from each step, different runs will yield slightly different results due to floating
point rounding errors (which make floating point addition a non-associative operation, `a + (b + c)
!= (a + b) + c`).

`Snarl` provides the following to allow both:

```@docs
Snarl.Storage.LocalStorage
```

This provides the following operations:

### Cleanup

```@docs
Snarl.Storage.clear!(storage::Snarl.Storage.LocalStorage, thread_id::Int)
```

### Access

```@docs
Snarl.Storage.get_value(storage::Snarl.Storage.LocalStorage, thread_id::Int=thread_id())
```

## Combined storage

In general, processing steps require multiple data items of the types described above. Snarl
therefore provides the following:

```@docs
Snarl.Storage.ParallelStorage
```

This provides the following operations:

### Adding

```@docs
Snarl.Storage.add_per_process!
Snarl.Storage.add_per_thread!
Snarl.Storage.add_per_step!
```

### Cleanup

```@docs
Snarl.Storage.clear_per_process!
Snarl.Storage.clear_per_thread!
Snarl.Storage.clear_per_step!
Snarl.Storage.clear!
```

Note that clearing the values still keeps their names in the container, that is, allows future
access which will re-create the value using the `make` function. It is also possible to completely
forget about the existence of the value, so future accesses will fail:

```@docs
Snarl.Storage.forget_per_process!
Snarl.Storage.forget_per_thread!
Snarl.Storage.forget_per_step!
Snarl.Storage.forget!
```

### Access

Accessing global data:

```@docs
Snarl.Storage.has_per_process
Snarl.Storage.get_per_process
Snarl.Storage.with_per_process
Snarl.Storage.get_lock
```

Accessing local data:

```@docs
Snarl.Storage.has_per_thread
Snarl.Storage.get_per_thread
Snarl.Storage.has_per_step
Snarl.Storage.get_per_step
```
