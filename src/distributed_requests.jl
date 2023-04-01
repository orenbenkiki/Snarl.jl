"""
Implement a (possibly) distributed request-response.

Since the `Distributed.Future` implementation isnt thread-safe (at least at the time I'm writing
this), we emulate it by using a channel that we send the result on. A small comfort here is that if
the computation is local we can use a local channel which presumably is more efficient.
"""
module DistributedRequests

using Distributed

export request_response

"""
    request_response(request_channel::Union{Channel,RemoteChannel},
                     response_channel::Channel)::Union{Channel,RemoteChannel}

Given a `request` channel to send a request through, and a `response` to listen through for the
response, return the properly wrapped response channel to send through the request channel so that
the service at the other side would be able to safely send the response back.

It would be much simpler to send a `Future` through the request channel, but `Future` objects are
not thread safe as of writing this code. If/when they become thread safe, this function should be
removed.
"""
function request_response(;
    request::Union{Channel,RemoteChannel},
    response::Channel,
)::Union{Channel,RemoteChannel}
    if request isa Channel
        return response
    else
        return RemoteChannel(() -> response)
    end
end

end # module
