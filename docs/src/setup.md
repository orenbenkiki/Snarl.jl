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

Which will be available everywhere. It will also allow you to initialize your own global variables
everywhere by invoking:

```@docs
Snarl.Launched.@send_everywhere
```

Which will be useful when setting up logging.

## Setting up logging

Logging from a multi-threaded and/or multi-process application requires some form of management.
`Snarl` provides the following:

```@docs
Snarl.DistributedLogging
```

To use this, on the main process (after launching all worker threads as described above), invoke:

```@docs
Snarl.DistributedLogging.central_log_channel
```

We can now use `send_everywhere` to publish the channel to the central logger to all the worker processors,
which allows us to everywhere use:

```@docs
Snarl.DistributedLogging.DistributedLogger
```

For example:

```
# This will spawn a local thread just to deal with log messages.
log_channel = central_log_channel()

# Configure the logging parameters based on command line options etc.
log_level = Logging.Info
show_time = true
base_time = now()

# Publish the above global variables everywhere.
@send_everywhere base_time base_time
@send_everywhere log_level log_level
@send_everywhere log_channel log_channel

# Use them to set up logging everywhere.
@everywhere begin
    using Base.Threads
    using Distributed
    using Logging
    using Snarl.DistributedLogging

    global_logger(DistributedLogger(
        log_channel,
        min_level = log_level,
        base_time = base_time,
    ))
end
```

After this incantation, you can use logging as usual everywhere (e.g., `@info "Foo"`) which will be
formatted on the invoking thread (indicating which process and thread it was generated in), then
forwarded to the central spawned thread on the main process, which will write the log message to the
stream passed to `central_log_channel` (by default, `stderr`). There would be no issue of garbled
messages due to multiple threads/processes logging at the same time; all messages from everywhere
would be serialized to a single stream.
