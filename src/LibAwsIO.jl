module LibAwsIO

import AwsC.LibAwsC: aws_allocator, aws_default_allocator, aws_byte_buf, aws_byte_cursor, aws_mem_acquire, aws_mem_release, aws_throw_error

# using aws_c_io_jll
const libaws_c_io = "/app/aws-crt/lib/libaws-c-io.so"

include("io_errors.jl")

function aws_io_library_init(allocator)
    @ccall libaws_c_io.aws_io_library_init(allocator::Ptr{aws_allocator})::Cint
end

const aws_event_loop_group = Cvoid

function aws_event_loop_group_new_default(alloc, max_threads, shutdown_options)
    @ccall libaws_c_io.aws_event_loop_group_new_default(alloc::Ptr{aws_allocator}, max_threads::UInt16, shutdown_options::Ptr{Cvoid})::Ptr{aws_event_loop_group}
end

const EVENT_LOOP_GROUP = Ref{Ptr{Cvoid}}(C_NULL)

const aws_event_loop = Cvoid

function aws_event_loop_group_get_next_loop(el_group)
    @ccall libaws_c_io.aws_event_loop_group_get_next_loop(el_group::Ptr{aws_event_loop_group})::Ptr{aws_event_loop}
end

const aws_shutdown_callback_options = Cvoid

mutable struct aws_host_resolver_default_options
    max_entries::Csize_t
    el_group::Ptr{aws_event_loop_group}
    shutdown_options::Ptr{aws_shutdown_callback_options}
    system_clock_override_fn::Ptr{Cvoid}
end

const aws_host_resolver = Cvoid

const HOST_RESOLVER = Ref{Ptr{Cvoid}}(C_NULL)

function aws_host_resolver_new_default(allocator, options)
    @ccall libaws_c_io.aws_host_resolver_new_default(allocator::Ptr{aws_allocator}, options::Ref{aws_host_resolver_default_options})::Ptr{aws_host_resolver}
end

struct aws_client_bootstrap_options
    event_loop_group::Ptr{aws_event_loop_group}
    host_resolver::Ptr{aws_host_resolver}
    host_resolution_config::Ptr{Cvoid} # Ptr{aws_host_resolution_config}
    on_shutdown_complete::Ptr{Cvoid}
    user_data::Ptr{Cvoid}
end

const aws_client_bootstrap = Cvoid

const CLIENT_BOOTSTRAP = Ref{Ptr{Cvoid}}(C_NULL)

function aws_client_bootstrap_new(allocator, options)
    @ccall libaws_c_io.aws_client_bootstrap_new(allocator::Ptr{aws_allocator}, options::Ref{aws_client_bootstrap_options})::Ptr{aws_client_bootstrap}
end

const aws_tls_ctx_options = Cvoid
const aws_tls_ctx = Cvoid
const aws_channel = Cvoid
const aws_channel_slot = Cvoid

@enum aws_task_status::Cint begin
    AWS_TASK_STATUS_RUN_READY = 0
    AWS_TASK_STATUS_CANCELLED = 1
end

@enum aws_channel_direction::Cint begin
    AWS_CHANNEL_DIR_READ = 0
    AWS_CHANNEL_DIR_WRITE = 1
end

mutable struct aws_channel_handler_vtable
    process_read_message::Ptr{Cvoid}
    process_write_message::Ptr{Cvoid}
    increment_read_window::Ptr{Cvoid}
    shutdown::Ptr{Cvoid}
    initial_window_size::Ptr{Cvoid}
    message_overhead::Ptr{Cvoid}
    destroy::Ptr{Cvoid}
end

mutable struct aws_channel_handler
    vtable::aws_channel_handler_vtable
    allocator::Ptr{aws_allocator}
    slot::Ptr{aws_channel_slot}
    impl::Any
end

const aws_io_message = Cvoid

get_message_data(msg::Ptr{aws_io_message}) = unsafe_load(Ptr{aws_byte_buf}(Ptr{UInt8}(msg) + sizeof(Ptr)))
set_message_data!(msg::Ptr{aws_io_message}, data::aws_byte_buf) = unsafe_store!(Ptr{aws_byte_buf}(Ptr{UInt8}(msg) + sizeof(Ptr)), data)
get_allocator(msg::Ptr{aws_io_message}) = unsafe_load(Ptr{Ptr{aws_allocator}}(msg))

function aws_channel_slot_increment_read_window(slot, size)
    @ccall libaws_c_io.aws_channel_slot_increment_read_window(slot::Ptr{aws_channel_slot}, size::Csize_t)::Cint
end

function aws_channel_slot_on_handler_shutdown_complete(slot, dir, error_code, abort_immediately)
    @ccall libaws_c_io.aws_channel_slot_on_handler_shutdown_complete(slot::Ptr{aws_channel_slot}, dir::Cint, error_code::Cint, abort_immediately::Bool)::Cint
end

function aws_channel_acquire_message_from_pool(channel, size_hint)
    @ccall libaws_c_io.aws_channel_acquire_message_from_pool(channel::Ptr{aws_channel}, 0::Cint, size_hint::Int32)::Ptr{aws_io_message}
end

function aws_channel_slot_send_message(slot, message, dir)
    @ccall libaws_c_io.aws_channel_slot_send_message(slot::Ptr{aws_channel_slot}, message::Ptr{Cvoid}, dir::Cint)::Cint
end

function aws_channel_slot_new(channel)
    @ccall libaws_c_io.aws_channel_slot_new(channel::Ptr{aws_channel})::Ptr{aws_channel_slot}
end

function aws_channel_slot_insert_end(channel, slot)
    @ccall libaws_c_io.aws_channel_slot_insert_end(channel::Ptr{aws_channel}, slot::Ptr{aws_channel_slot})::Cint
end

function aws_channel_slot_insert_right(slot, to_add)
    @ccall libaws_c_io.aws_channel_slot_insert_right(slot::Ptr{aws_channel_slot}, to_add::Ptr{aws_channel_slot})::Cint
end

function aws_channel_slot_insert_left(slot, to_add)
    @ccall libaws_c_io.aws_channel_slot_insert_left(slot::Ptr{aws_channel_slot}, to_add::Ptr{aws_channel_slot})::Cint
end

function aws_channel_get_first_slot(channel)
    @ccall libaws_c_io.aws_channel_get_first_slot(channel::Ptr{aws_channel})::Ptr{aws_channel_slot}
end

function aws_channel_slot_set_handler(slot, handler)
    @ccall libaws_c_io.aws_channel_slot_set_handler(slot::Ptr{Cvoid}, handler::Any)::Cint
end

function aws_channel_slot_set_handler(slot, handler::Ptr{Cvoid})
    @ccall libaws_c_io.aws_channel_slot_set_handler(slot::Ptr{aws_channel_slot}, handler::Ptr{Cvoid})::Cint
end

function aws_tls_client_ctx_new(alloc, options)
    @ccall libaws_c_io.aws_tls_client_ctx_new(alloc::Ptr{aws_allocator}, options::Ptr{aws_tls_ctx_options})::Ptr{aws_tls_ctx}
end

const aws_tls_connection_options = Cvoid

function aws_tls_connection_options_init_from_ctx(conn_options, ctx)
    @ccall libaws_c_io.aws_tls_connection_options_init_from_ctx(conn_options::Ref{aws_tls_connection_options}, ctx::Ptr{aws_tls_ctx})::Cvoid
end

function aws_tls_ctx_options_init_client_mtls_from_path(options, allocator, cert_path, pkey_path)
    @ccall libaws_c_io.aws_tls_ctx_options_init_client_mtls_from_path(options::Ptr{aws_tls_ctx_options}, allocator::Ptr{aws_allocator}, cert_path::Ptr{Cchar}, pkey_path::Ptr{Cchar})::Cint
end

function aws_tls_ctx_options_init_client_mtls_from_system_path(options, allocator, cert_reg_path)
    @ccall libaws_c_io.aws_tls_ctx_options_init_client_mtls_from_system_path(options::Ptr{aws_tls_ctx_options}, allocator::Ptr{aws_allocator}, cert_reg_path::Ptr{Cchar})::Cint
end

function aws_tls_ctx_options_override_default_trust_store_from_path(options, ca_path, ca_file)
    @ccall libaws_c_io.aws_tls_ctx_options_override_default_trust_store_from_path(options::Ptr{aws_tls_ctx_options}, ca_path::Ptr{Cchar}, ca_file::Ptr{Cchar})::Cint
end

function aws_tls_ctx_options_init_default_client(options, allocator)
    @ccall libaws_c_io.aws_tls_ctx_options_init_default_client(options::Ptr{aws_tls_ctx_options}, allocator::Ptr{aws_allocator})::Cvoid
end

function aws_tls_ctx_options_set_alpn_list(options, alpn_list)
    @ccall libaws_c_io.aws_tls_ctx_options_set_alpn_list(options::Ptr{aws_tls_ctx_options}, alpn_list::Ptr{Cchar})::Cint
end

function aws_tls_ctx_options_set_verify_peer(options, verify_peer)
    @ccall libaws_c_io.aws_tls_ctx_options_set_verify_peer(options::Ptr{aws_tls_ctx_options}, verify_peer::Bool)::Cvoid
end

function aws_tls_connection_options_set_server_name(conn_options, allocator, server_name)
    @ccall libaws_c_io.aws_tls_connection_options_set_server_name(conn_options::Ptr{aws_tls_connection_options}, allocator::Ptr{aws_allocator}, server_name::Ref{aws_byte_cursor})::Cint
end

function aws_tls_connection_options_clean_up(connection_options)
    @ccall libaws_c_io.aws_tls_connection_options_clean_up(connection_options::Ref{aws_tls_connection_options})::Cvoid
end

function aws_tls_ctx_release(ctx)
    @ccall libaws_c_io.aws_tls_ctx_release(ctx::Ptr{aws_tls_ctx})::Cvoid
end

function aws_tls_ctx_options_clean_up(options)
    @ccall libaws_c_io.aws_tls_ctx_options_clean_up(options::Ptr{aws_tls_ctx_options})::Cvoid
end

const aws_channel_task = Cvoid

#NOTE: task_fn needs to release channel_task memory
function aws_channel_task_init(task_fn, arg, type_tag)
    channel_task = aws_mem_acquire(aws_default_allocator(), 512)
    @ccall libaws_c_io.aws_channel_task_init(channel_task::Ptr{aws_channel_task}, task_fn::Ptr{Cvoid}, arg::Any, type_tag::Ptr{Cchar})::Cvoid
    return channel_task
end

function aws_channel_schedule_task_now(channel, task)
    @ccall libaws_c_io.aws_channel_schedule_task_now(channel::Ptr{aws_channel}, task::Ptr{aws_channel_task})::Cint
end

@enum aws_socket_type::UInt32 begin
    AWS_SOCKET_STREAM = 0
    AWS_SOCKET_DGRAM = 1
end

@enum aws_socket_domain::UInt32 begin
    AWS_SOCKET_IPV4 = 0
    AWS_SOCKET_IPV6 = 1
    AWS_SOCKET_LOCAL = 2
    AWS_SOCKET_VSOCK = 3
end

mutable struct aws_socket_options
    type::aws_socket_type
    domain::aws_socket_domain
    connect_timeout_ms::UInt32
    keep_alive_interval_sec::UInt16
    keep_alive_timeout_sec::UInt16
    keep_alive_max_failed_probes::UInt16
    keepalive::Bool
end

aws_socket_options() = aws_socket_options(AWS_SOCKET_STREAM, AWS_SOCKET_IPV4, 3000, 0, 0, 0, false)

mutable struct aws_socket_channel_bootstrap_options
    bootstrap::Ptr{aws_client_bootstrap}
    host_name::Cstring
    port::UInt32
    socket_options::aws_socket_options
    tls_options::Ptr{aws_tls_connection_options}
    creation_callback::Ptr{Cvoid}
    setup_callback::Ptr{Cvoid}
    shutdown_callback::Ptr{Cvoid}
    enable_read_back_pressure::Bool
    user_data::Any
    requested_event_loop::Ptr{aws_event_loop}
    host_resolution_override_config::Ptr{Cvoid} # Ptr{aws_host_resolution_config}
    host::String # to hold the reference to our host_name Cstring
end

function aws_client_bootstrap_new_socket_channel(options)
    @ccall libaws_c_io.aws_client_bootstrap_new_socket_channel(options::Ref{aws_socket_channel_bootstrap_options})::Cint
end

function aws_tls_connection_options_set_callbacks(options, on_negotiation_result, user_data)
    @ccall libaws_c_io.aws_tls_connection_options_set_callbacks(options::Ptr{aws_tls_connection_options}, on_negotiation_result::Ptr{Cvoid}, C_NULL::Ptr{Cvoid}, C_NULL::Ptr{Cvoid}, user_data::Any)::Cvoid
end

# NOTE: caller is responsible for cleaning up tls_options
function tlsoptions(host_str::String;
    allocator=aws_default_allocator(),
    # tls options
    ssl_cert=nothing,
    ssl_key=nothing,
    ssl_capath=nothing,
    ssl_cacert=nothing,
    ssl_insecure=false,
    ssl_alpn_list=nothing,
    on_negotiation_result=C_NULL,
    user_data=C_NULL
)
    tls_options = aws_mem_acquire(allocator, 64)
    tls_ctx_options = aws_mem_acquire(allocator, 512)
    tls_ctx = C_NULL
    try
        if ssl_cert !== nothing && ssl_key !== nothing
            aws_tls_ctx_options_init_client_mtls_from_path(tls_ctx_options, allocator, ssl_cert, ssl_key) != 0 && aws_throw_error()
        elseif Sys.iswindows() && ssl_cert !== nothing && ssl_key === nothing
            aws_tls_ctx_options_init_client_mtls_from_system_path(tls_ctx_options, allocator, ssl_cert) != 0 && aws_throw_error()
        else
            aws_tls_ctx_options_init_default_client(tls_ctx_options, allocator)
        end
        if ssl_capath !== nothing && ssl_cacert !== nothing
            aws_tls_ctx_options_override_default_trust_store_from_path(tls_ctx_options, ssl_capath, ssl_cacert) != 0 && aws_throw_error()
        end
        if ssl_insecure
            aws_tls_ctx_options_set_verify_peer(tls_ctx_options, false)
        end
        if ssl_alpn_list !== nothing
            aws_tls_ctx_options_set_alpn_list(tls_ctx_options, ssl_alpn_list) != 0 && aws_throw_error()
        end
        tls_ctx = aws_tls_client_ctx_new(allocator, tls_ctx_options)
        tls_ctx == C_NULL && aws_throw_error()
        aws_tls_connection_options_init_from_ctx(tls_options, tls_ctx)
        aws_tls_connection_options_set_server_name(tls_options, allocator, aws_byte_cursor(host_str)) != 0 && aws_throw_error()
        if on_negotiation_result !== C_NULL
            aws_tls_connection_options_set_callbacks(tls_options, on_negotiation_result, user_data)
        end
    finally
        aws_tls_ctx_options_clean_up(tls_ctx_options)
        aws_tls_ctx_release(tls_ctx)
        aws_mem_release(allocator, tls_ctx_options)
    end
    return tls_options
end

function aws_socket_channel_bootstrap_options(host::AbstractString, port::Integer;
    tls::Bool=false,
    allocator=aws_default_allocator(),
    bootstrap=CLIENT_BOOTSTRAP[],
    socket_options=aws_socket_options(),
    # tls options
    ssl_cert=nothing,
    ssl_key=nothing,
    ssl_capath=nothing,
    ssl_cacert=nothing,
    ssl_insecure=false,
    ssl_alpn_list="h2;http/1.1",
    # callbacks
    creation_callback=C_NULL,
    setup_callback=C_NULL,
    shutdown_callback=C_NULL,
    user_data=C_NULL,
    enabled_read_back_pressure=false,
    )
    host_str = String(host)
    tls_options = C_NULL
    if tls
        tls_options = tlsoptions(
            host_str;
            allocator=allocator,
            ssl_cert=ssl_cert,
            ssl_key=ssl_key,
            ssl_capath=ssl_capath,
            ssl_cacert=ssl_cacert,
            ssl_insecure=ssl_insecure,
            ssl_alpn_list=ssl_alpn_list
        )
    end
    x = aws_socket_channel_bootstrap_options(
        bootstrap,
        Base.unsafe_convert(Cstring, host_str),
        UInt32(port),
        socket_options,
        tls_options,
        creation_callback,
        setup_callback,
        shutdown_callback,
        enabled_read_back_pressure,
        user_data,
        C_NULL,
        C_NULL,
        host_str
    )
    finalizer(x) do x
        if x.tls_options !== C_NULL
            aws_tls_connection_options_clean_up(x.tls_options)
            aws_mem_release(allocator, x.tls_options)
        end
    end
    return x
end

function aws_tls_client_handler_new(allocator, options, slot)
    @ccall libaws_c_io.aws_tls_client_handler_new(allocator::Ptr{aws_allocator}, options::Ptr{aws_tls_connection_options}, slot::Ptr{aws_channel_slot})::Ptr{Cvoid}
end

function aws_tls_client_handler_start_negotiation(handler)
    @ccall libaws_c_io.aws_tls_client_handler_start_negotiation(handler::Ptr{Cvoid})::Cint
end

function aws_channel_shutdown(channel, error_code)
    @ccall libaws_c_io.aws_channel_shutdown(channel::Ptr{aws_channel}, error_code::Cint)::Cint
end

precompiling() = ccall(:jl_generating_output, Cint, ()) == 1

function __init__()
    if !precompiling()
        allocator = aws_default_allocator()
        aws_io_library_init(allocator)
        el_group = aws_event_loop_group_new_default(allocator, 0, C_NULL)
        EVENT_LOOP_GROUP[] = el_group
        # populate default host resolver
        resolver_options = aws_host_resolver_default_options(8, el_group, C_NULL, C_NULL)
        host_resolver = aws_host_resolver_new_default(allocator, resolver_options)
        @assert host_resolver != C_NULL
        HOST_RESOLVER[] = host_resolver
        # populate default client bootstrap w/ event loop, host resolver, and allocator
        bootstrap_options = aws_client_bootstrap_options(el_group, host_resolver, C_NULL, C_NULL, C_NULL)
        CLIENT_BOOTSTRAP[] = aws_client_bootstrap_new(allocator, bootstrap_options)
        @assert CLIENT_BOOTSTRAP[] != C_NULL
    end
end

end