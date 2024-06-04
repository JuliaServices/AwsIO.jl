module AwsIO

module Sockets

using Logging, LibAwsCommon, LibAwsIO

struct SocketError <: Exception
    msg::String
end

sockerr(msg::String) = CapturedException(SocketError(msg), Base.backtrace())
sockerr(code::Integer) = sockerr(unsafe_string(aws_error_str(code)))
sockerr(e::Exception) = CapturedException(e, Base.backtrace())

# NOTE: caller is responsible for cleaning up tls_options
function tlsoptions(host::String;
    allocator=default_aws_allocator(),
    # tls options
    ssl_cert=nothing,
    ssl_key=nothing,
    ssl_capath=nothing,
    ssl_cacert=nothing,
    ssl_insecure=false,
    ssl_alpn_list=nothing,
    on_negotiation_result=C_NULL,
    on_negotiation_result_user_data=C_NULL
)
    tls_options = aws_tls_connection_options(C_NULL, C_NULL, C_NULL, C_NULL, C_NULL, C_NULL, C_NULL, false, UInt32(0))
    tls_ctx_options = Ptr{aws_tls_ctx_options}(aws_mem_acquire(allocator, sizeof(aws_tls_ctx_options)))
    tls_ctx = C_NULL
    try
        if ssl_cert !== nothing && ssl_key !== nothing
            aws_tls_ctx_options_init_client_mtls_from_path(tls_ctx_options, allocator, ssl_cert, ssl_key) != 0 && sockerr("aws_tls_ctx_options_init_client_mtls_from_path failed")
        elseif Sys.iswindows() && ssl_cert !== nothing && ssl_key === nothing
            aws_tls_ctx_options_init_client_mtls_from_system_path(tls_ctx_options, allocator, ssl_cert) != 0 && sockerr("aws_tls_ctx_options_init_client_mtls_from_system_path failed")
        else
            aws_tls_ctx_options_init_default_client(tls_ctx_options, allocator)
        end
        if ssl_capath !== nothing && ssl_cacert !== nothing
            aws_tls_ctx_options_override_default_trust_store_from_path(tls_ctx_options, ssl_capath, ssl_cacert) != 0 && sockerr("aws_tls_ctx_options_override_default_trust_store_from_path failed")
        end
        if ssl_insecure
            aws_tls_ctx_options_set_verify_peer(tls_ctx_options, false)
        end
        if ssl_alpn_list !== nothing
            aws_tls_ctx_options_set_alpn_list(tls_ctx_options, ssl_alpn_list) != 0 && sockerr("aws_tls_ctx_options_set_alpn_list failed")
        end
        tls_ctx = aws_tls_client_ctx_new(allocator, tls_ctx_options)
        tls_ctx == C_NULL && sockerr("")
        ref = Ref(tls_options)
        host_ref = Ref(aws_byte_cursor(sizeof(host), pointer(host)))
        aws_tls_connection_options_init_from_ctx(ref, tls_ctx)
        aws_tls_connection_options_set_server_name(ref, allocator, host_ref) != 0 && sockerr("aws_tls_connection_options_set_server_name failed")
        if on_negotiation_result !== C_NULL
            aws_tls_connection_options_set_callbacks(ref, on_negotiation_result, C_NULL, C_NULL, pointer_from_objref(on_negotiation_result_user_data))
        end
        tls_options = ref[]
    finally
        aws_tls_ctx_options_clean_up(tls_ctx_options)
        aws_tls_ctx_release(tls_ctx)
        aws_mem_release(allocator, tls_ctx_options)
    end
    return tls_options
end

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
    ON_NEGOTIATION_RESULT[] = @cfunction(c_on_negotiation_result, Cvoid, (Ptr{Cvoid}, Ptr{aws_channel_slot}, Cint, Any))
    TLS_UPGRADE[] = @cfunction(c_tls_upgrade, Cvoid, (Ptr{aws_channel_task}, Any, Cint))
    return
end

end # module Sockets

end # module AwsIO