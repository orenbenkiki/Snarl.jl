# Initializing the application

## Launching worker processes

The 1st thing the application will need to do is to launch the worker processes. In a reasonable
production system, the application will want to launch one process on each (additional) server in
some cluster, such that it will use multiple threads (processors) of that server - ideally, all of
them.

Compute clusters are subtle and quick to anger; each has its own particular way to obtain multiple
servers for an application. This might be as simple as invoking `ssh` to execute a process on each
server in some , or may require complex incantation for obtaining server access from some cluster
management software, such as spinning up instances in some cloud provider.

Either way, this phase is outside the scope of `Snarl`; all it requires is that (if you want a
distributed application) you will invoke `Distributed.addprocs`, ideally spawning one process per
server, which uses multiple threads (so if you use BLAS, specify `enable_threaded_blas=true`).

Once this is done, you will need to use:

```@docs
Snarl.Launched
```

To tell `Snarl` you have launched the worker processes, invoke:

```@docs
Snarl.Launched.launched
```

That is, write:

```
@everywhere using Snarl.Launched
launched()
```

Note you should do this even if you did not launch any worker processes; this would restrict the
application to using the threads of a single physical machine. This is still useful as a single
server machine can be very powerful these days (dozens of threads, dozens of hundreds of GBs of
RAM). Even humble laptops can be pretty powerful (several threads, dozens of GBs of RAM). And
writing a pure multi-threaded application is simpler than a multi-process, multi-threaded one. That
said, if this is all you want to do, then `Snarl` might be an overkill. You might want to use it in
order to have a smooth migration path towards running the application on a cluster of servers.

Either way, this will cause `Snarl` to figure out what is available. Specifically, it will
initialize everywhere:

```@docs
Snarl.Launched.total_threads_count
Snarl.Launched.threads_count_of_processes
```
## Setting up logging

Logging from a multi-threaded and/or multi-process application requires some form of management.
`Snarl` provides the following:

```@docs
Snarl.DistributedLogging
```

To use this, on the main process (after launching all worker threads as described above), invoke:

```@docs
Snarl.DistributedLogging.setup_logging
```

This will set the `global_logger` in each process to an instance of:

```@docs
Snarl.DistributedLogging.DistributedLogger
```

This will use a channel to send all log messages to a task (which is run on the main process), which will print
them one at a time to the stream you provide. You can also (only from the main process):

```@docs
Snarl.DistributedLogging.drain_logging
Snarl.DistributedLogging.teardown_logging
```

For example:

```
# Configure the logging parameters (typically based on command line options).
setup_logging(log_level = Logging.info, show_time = true, base_time = now())
```

After this incantation, you can use logging as usual everywhere (e.g., `@info "Foo"`) which will be
formatted on the invoking thread (indicating which process and thread it was generated in), then
forwarded to the central spawned task on the main process, which will write the log message to the
stream passed to `setup_logging` (by default, `stderr`). There would be no issue of garbled messages
due to multiple threads/processes logging at the same time; all messages from everywhere would be
serialized to a single stream.

In general `Snarl` requires us to be able to communicate between multiple threads of arbitrary
processes. A common pattern is to send a request to a service worker thread and wait for a response.
Ideally, one would like to use `Distributed.Future` to implement this idiom, but we can't since
(at the time of writing this), `Distributed.Future` was not thread-safe(!). Instead, we use:

```@docs
Snarl.DistributedRequests
```

Which creates a channel for the (possibly remote) service to send the single response to the
requester. A minor comfort here is that we can use a local channel if both the requester and the
service run on the same machine (in different threads of the same process). This is implemented
by:

```@docs
Snarl.DistributedRequests.request_response
```

## Setting up locks

When running a distributed application across multiple physical processors, it is sometimes
necessary to coordinate tasks. However, normal Julia locks are not up to the task, as their scope is
restricted to a single process. To cover this gap, `Snarl` provides a distributed locks mechanism.

```@docs
Snarl.DistributedLocks
```

To use this, on the main process (after launching all worker threads as described above), invoke:

```@docs
Snarl.DistributedLocks.setup_locks
```

You can control the spawned task, similarly to the logging case above:

```@docs
Snarl.DistributedLocks.drain_locks
Snarl.DistributedLocks.teardown_locks
Snarl.DistributedLocks.forget_locks
```

To use this, invoke `with_lock`:

```@docs
Snarl.DistributedLocks.with_distributed_lock
```

For example:

```
with_distributed_lock() do is_first
    ... Critical section across all processes and threads ...
end
```

Providing `is_first` is useful when code on different processes wants to create a distributed shared
resource (e.g., generate some file which can be accessed by all the worker processes using a shared
networked file system). The idiom is as follows:

```
if resource_does_not_exist()
    with_distributed_lock(name_of_resource) do is_first
        if is_first
            ... actually create the resource, which may take a long time ...
        else
            ... resource was created by someone else; we have exclusive access to it here ...
        end
    end
end
... resource is now guaranteed to have been created ...
```
