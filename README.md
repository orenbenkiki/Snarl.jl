# Snarl - An opinionated framework for parallel processing.

Julia provides excellent capabilities for running code in multiple threads, and for running code in
multiple processes. However, as far as I could find, it doesn't provide a convenient abstraction for
combining the two. This combination comes up when one wishes to utilize a compute cluster containing
multiple servers, each with multiple processors.

Trying to do this manually quickly results in adding a lot of complexity to the application. Ideally
Julia would provide a proper cross-processes work-stealing scheduler, but this is a major
undertaking, given that shared memory would only work between threads running in the same process
(physical machine).

`Snarl` tries to provide a simplistic and convenient API for such applications. It is also usable
for purely multi-threaded applications that run in a single process (that is, running an application
on your laptop instead of on your compute server), and even for pure multi-process applications
(using a single thread on each process), though it isn't clear why you'd want to do that. In both
cases, these configurations are considered to be special cases of the general multi-threaded in
multi-process configuration.
