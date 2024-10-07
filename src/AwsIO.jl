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

const LOGGER_FILE_REF = Ref{Libc.FILE}()
const LOGGER_OPTIONS = Ref{aws_logger_standard_options}()
const LOGGER = Ref{Ptr{Cvoid}}(C_NULL)

#NOTE: this is global process logging in the aws-crt libraries; not appropriate for request-level
# logging, but more for debugging the library itself
function set_log_level!(level::Integer)
    @assert 0 <= level <= 7 "log level must be between 0 and 7"
    @assert aws_logger_set_log_level(LOGGER[], aws_log_level(level)) == 0
    return
end

function trace_memory!()
    LibAwsCommon.set_default_aws_allocator!(LibAwsCommon.mem_trace_allocator())
    return
end

function trace_memory_dump()
    aws_mem_tracer_dump(LibAwsCommon.default_aws_allocator())
end

function __init__()
    allocator = default_aws_allocator()
    LOGGER[] = aws_mem_acquire(allocator, 64)
    LOGGER_FILE_REF[] = Libc.FILE(Libc.RawFD(1), "w")
    LOGGER_OPTIONS[] = aws_logger_standard_options(aws_log_level(3), C_NULL, Ptr{Libc.FILE}(LOGGER_FILE_REF[].ptr))
    @assert aws_logger_init_standard(LOGGER[], allocator, LOGGER_OPTIONS) == 0
    aws_logger_set(LOGGER[])
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