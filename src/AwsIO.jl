module AwsIO

module Sockets

using Logging, LibAwsCommon, LibAwsIO

struct SocketError <: Exception
    msg::String
end

sockerr(msg::String) = CapturedException(SocketError(msg), Base.backtrace())
sockerr(code::Integer) = sockerr(unsafe_string(aws_error_str(code)))
sockerr(e::Exception) = CapturedException(e, Base.backtrace())

include("sockets/client.jl")

function __init__()
    SETUP_CALLBACK[] = @cfunction(c_setup_callback, Cvoid, (Ptr{aws_client_bootstrap}, Cint, Ptr{aws_channel}, Any))
    SHUTDOWN_CALLBACK[] = @cfunction(c_shutdown_callback, Cvoid, (Ptr{aws_client_bootstrap}, Cint, Ptr{aws_channel}, Any))
    SCHEDULED_WRITE[] = @cfunction(c_scheduled_write, Cvoid, (Ptr{aws_channel_task}, Any, Cint))
    RW_HANDLER_VTABLE[] = aws_channel_handler_vtable(
        @cfunction(c_process_read_message, Cint, (Ref{aws_channel_handler}, Ptr{aws_channel_slot}, Ptr{aws_io_message})),
        @cfunction(c_process_write_message, Cint, (Ref{aws_channel_handler}, Ptr{aws_channel_slot}, Ptr{aws_io_message})),
        @cfunction(c_increment_read_window, Cint, (Ref{aws_channel_handler}, Ptr{aws_channel_slot}, Csize_t)),
        @cfunction(c_shutdown, Cint, (Ref{aws_channel_handler}, Ptr{aws_channel_slot}, aws_channel_direction, Cint, Bool)),
        @cfunction(c_initial_window_size, Csize_t, (Ref{aws_channel_handler},)),
        @cfunction(c_message_overhead, Csize_t, (Ref{aws_channel_handler},)),
        @cfunction(c_destroy, Cvoid, (Ref{aws_channel_handler},)),
        C_NULL,
        C_NULL,
        C_NULL
    )
    INCREMENT_READ_WINDOW_TASK[] = @cfunction(c_increment_read_window_task, Cvoid, (Ptr{aws_channel_task}, Any, Cint))
    ON_NEGOTIATION_RESULT[] = @cfunction(c_on_negotiation_result, Cvoid, (Ptr{Cvoid}, Ptr{aws_channel_slot}, Cint, Any))
    TLS_UPGRADE[] = @cfunction(c_tls_upgrade, Cvoid, (Ptr{aws_channel_task}, Any, Cint))
    return
end

end # module Sockets

end # module AwsIO