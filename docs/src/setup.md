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

If you did launch worker processes, you should write:

```
@everywhere using Snarl.Launched
launched()
```

Note you should invoked `launched()` even if you did not launch any worker processes. This would
restrict the application to using the threads of a single physical machine. That said, a single
server machine can be very powerful these days (dozens of threads, dozens of hundreds of GBs of
RAM), and even humble laptops can be pretty powerful (several threads, dozens of GBs of RAM). And
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
# Configure the logging parameters based on command line options etc.
setup_logging(log_level = Logging.info, show_time = true, base_time = now())
```

After this incantation, you can use logging as usual everywhere (e.g., `@info "Foo"`) which will be
formatted on the invoking thread (indicating which process and thread it was generated in), then
forwarded to the central spawned task on the main process, which will write the log message to the
stream passed to `setup_logging` (by default, `stderr`). There would be no issue of garbled messages
due to multiple threads/processes logging at the same time; all messages from everywhere would be
serialized to a single stream.

In general `Snarl` requires us to be able to communicate between multiple threads of arbitrary
(possibly different processes); logging is a trivial example. For historical reasons, Julia does
**not** provide this functionality out of the box. Internally `Snarl` uses:

```@docs
Snarl.Channels
Snarl.Channels.ThreadSafeRemoteChannel
```

However, this is **not** a general solution to the problem; it only works when the remote channel it
is known to communicate with a different process, not with the current one.

In addition, note you can't freely pass `Future` object across channels, because they are likewise
**not** thread-safe. For the common pattern where a thread on some process is listening to requests
on a channel and needs to send responses back, when the requesters might be other threads on the
same process or remote processes, create a `Channel` to get the response on instead of creating a
`Future` to wait for. Then, instead of sending this response channel as-is through the request
channel, send the results of `request_response` instead; this will wrap the response channel in a
`RemoteChannel` if the request channel leads to a different process, otherwise it will keep the
response channel as-is for fast communication within the same process.

```@docs
Snarl.Channels.ThreadSafeRemoteChannel
Snarl.Channels.request_response
```
