struct FieldRef{T, S}
    x::T
    field::Symbol
end

FieldRef(x::T, field::Symbol) where {T} = FieldRef{T, fieldtype(T, field)}(x, field)

function Base.unsafe_convert(P::Union{Type{Ptr{T}},Type{Ptr{Cvoid}}}, x::FieldRef{S, T}) where {T, S}
    @assert isconcretetype(S) && ismutabletype(S) "only fields of mutable types are supported with FieldRef"
    return P(pointer_from_objref(x.x) + fieldoffset(S, Base.fieldindex(S, x.field)))
end

Base.pointer(x::FieldRef{S, T}) where {S, T} = Base.unsafe_convert(Ptr{T}, x)

struct StructRef{T}
    ptr::Ptr{T}
end

function Base.getproperty(x::StructRef{T}, k::Symbol) where {T}
    @assert isconcretetype(T) "only concrete struct types are supported with StructRef"
    return unsafe_load(Ptr{fieldtype(T, k)}(Ptr{UInt8}(getfield(x, :ptr)) + fieldoffset(T, Base.fieldindex(T, k))))
end

function Base.setproperty!(x::StructRef{T}, k::Symbol, v::S) where {T, S}
    @assert isconcretetype(T) "only concrete struct types are supported with StructRef"
    @assert fieldtype(T, k) == S "field type mismatch"
    unsafe_store!(Ptr{S}(Ptr{UInt8}(getfield(x, :ptr)) + fieldoffset(T, Base.fieldindex(T, k))), v)
    return v
end

const DEFAULT_READ_BUFFER_SIZE = 1 * 1024 * 1024  # 1MB

mutable struct Client
    host::String
    port::Int
    debug::Bool
    tls::Bool
    socket_options::aws_socket_options
    tls_options::Union{aws_tls_connection_options, Nothing}
    buffer_capacity::Int
    channel::Ptr{aws_channel}
    slot::Ptr{aws_channel_slot}
    ch::Channel{Symbol}
    readbuf::Base.BufferStream
    writelock::ReentrantLock
    writebuf::IOBuffer
    bootstrap::aws_socket_channel_bootstrap_options
    handler::aws_channel_handler

    function Client(host, port;
        allocator=default_aws_allocator(),
        client_bootstrap=default_aws_client_bootstrap(),
        buffer_capacity::Int=DEFAULT_READ_BUFFER_SIZE,
        # socket options
        socket_options::Union{aws_socket_options, Nothing}=nothing,
        socket_type::aws_socket_type=AWS_SOCKET_STREAM,
        socket_domain::aws_socket_domain=AWS_SOCKET_IPV4,
        connect_timeout_ms::Integer=3000,
        keep_alive_interval_sec::Integer=0,
        keep_alive_timeout_sec::Integer=0,
        keep_alive_max_failed_probes::Integer=0,
        keepalive::Bool=false,
        # tls options
        tls::Bool=false,
        tls_options::Union{aws_tls_connection_options, Nothing}=nothing,
        ssl_cert=nothing,
        ssl_key=nothing,
        ssl_capath=nothing,
        ssl_cacert=nothing,
        ssl_insecure=false,
        ssl_alpn_list="h2;http/1.1",
        enable_read_back_pressure::Bool=false,
        requested_event_loop=C_NULL,
        host_resolution_override_config=C_NULL,
        debug::Bool=false)
        host = String(host)
        port = Int(port)
        if socket_options === nothing
            socket_options = aws_socket_options(
                socket_type,
                socket_domain,
                connect_timeout_ms,
                keep_alive_interval_sec,
                keep_alive_timeout_sec,
                keep_alive_max_failed_probes,
                keepalive,
                ntuple(i -> Cchar(0), 16)
            )
        end
        if tls && tls_options === nothing
            tls_options = LibAwsIO.tlsoptions(host;
                ssl_cert,
                ssl_key,
                ssl_capath,
                ssl_cacert,
                ssl_insecure,
                ssl_alpn_list
            )
        end
        x = new(host, port, debug, tls, socket_options, tls_options, buffer_capacity, C_NULL, C_NULL, Channel{Symbol}(0), Base.BufferStream(), ReentrantLock(), PipeBuffer())
        GC.@preserve x begin
            x.bootstrap = aws_socket_channel_bootstrap_options(
                client_bootstrap,
                Base.unsafe_convert(Cstring, host),
                port % UInt32,
                pointer(FieldRef(x, :socket_options)),
                tls_options !== nothing ? pointer(FieldRef(x, :tls_options)) : C_NULL,
                C_NULL,
                SETUP_CALLBACK[],
                SHUTDOWN_CALLBACK[],
                enable_read_back_pressure,
                pointer_from_objref(x),
                requested_event_loop,
                host_resolution_override_config
            )
            aws_client_bootstrap_new_socket_channel(FieldRef(x, :bootstrap)) != 0 && throw(ClientError("failed to create socket"))
            @assert take!(x.ch) == :setup "failed to create socket" # wait for connection
            return x
        end
    end
end

function c_process_read_message(handler, slot, messageptr)::Cint
    msg = StructRef(messageptr)
    data = msg.message_data
    sock = unsafe_pointer_to_objref(handler.impl)
    GC.@preserve sock begin
        sock.debug && @info "c_process_read_message: $(data.len) bytes"
        ret = AWS_OP_ERR
        try
            unsafe_write(sock.readbuf, data.buffer, data.len)
            ret = AWS_OP_SUCCESS
            aws_mem_release(msg.allocator, messageptr)
        catch e
            close(sock.ch, sockerr(e))
        end
        return ret
    end
end

function c_process_write_message(handler, slot, messageptr)::Cint
    # this should never be called since we only want to be the last slot in the channel
    return AWS_OP_ERR
end

function c_increment_read_window(handler, slot, size)::Cint
    aws_channel_slot_increment_read_window(slot, size)
    return AWS_OP_SUCCESS
end

function c_shutdown(handler, slot, dir, error_code, free_scarce_resources_immediately)::Cint
    sock = unsafe_pointer_to_objref(handler.impl)
    GC.@preserve sock begin
        close(sock.ch, sockerr(error_code))
    end
    return aws_channel_slot_on_handler_shutdown_complete(slot, dir, error_code, free_scarce_resources_immediately)
end

function c_initial_window_size(handler)::Csize_t
    sock = unsafe_pointer_to_objref(handler.impl)
    GC.@preserve sock begin
        # Return the buffer capacity as the initial window size
        return sock.buffer_capacity
    end
end

function c_message_overhead(channel_handler)::Csize_t
    return 0
end

function c_destroy(channel_handler)
    return
end

function c_reset_statistics(channel_handler)::Cvoid
    return AWS_OP_SUCCESS
end

function c_gather_statistics(channel_handler, stats#=::Ptr{aws_array_list}=#)::Cvoid
    return AWS_OP_SUCCESS
end

function c_trigger_read(channel_handler)::Cvoid
    return AWS_OP_SUCCESS
end

const RW_HANDLER_VTABLE = Ref{aws_channel_handler_vtable}()

function c_setup_callback(bootstrap, error_code, channel, socket)
    GC.@preserve socket begin
        if error_code != 0
            socket.debug && @error "c_setup_callback: error = '$(unsafe_string(aws_error_str(error_code)))'"
            close(socket.ch, sockerr(error_code))
        else
            slot = aws_channel_slot_new(channel)
            if slot == C_NULL
                socket.debug && @error "c_setup_callback: failed to create channel slot"
                close(socket.ch, sockerr("failed to create channel slot"))
            end
            if aws_channel_slot_insert_end(channel, slot) != 0
                socket.debug && @error "c_setup_callback: failed to insert channel slot"
                close(socket.ch, sockerr("failed to insert channel slot"))
            end
            socket.handler = aws_channel_handler(Base.unsafe_convert(Ptr{aws_channel_handler_vtable}, RW_HANDLER_VTABLE), default_aws_allocator(), C_NULL, pointer_from_objref(socket))
            if aws_channel_slot_set_handler(slot, FieldRef(socket, :handler)) != 0
                socket.debug && @error "c_setup_callback: failed to set channel slot handler"
                close(socket.ch, sockerr("failed to set channel slot handler"))
            end
            socket.channel = channel
            socket.slot = slot
            put!(socket.ch, :setup)
        end
    end
    return
end

const SETUP_CALLBACK = Ref{Ptr{Cvoid}}(C_NULL)

function c_shutdown_callback(bootstrap, error_code, channel, socket)
    GC.@preserve socket begin
        socket.debug && @warn "c_shutdown_callback"
        close(socket.ch)
        close(socket.readbuf)
        close(socket.writebuf)
        socket.channel = C_NULL
        socket.slot = C_NULL
    end
    return
end

const SHUTDOWN_CALLBACK = Ref{Ptr{Cvoid}}(C_NULL)

function schedule_channel_task(channel, task_fn, arg, type_tag)
    ch_task = Ptr{aws_channel_task}(aws_mem_acquire(default_aws_allocator(), Base._counttuple(fieldtype(aws_channel_task, :data))))
    aws_channel_task_init(ch_task, task_fn, arg, type_tag)
    aws_channel_schedule_task_now(channel, ch_task)
end

function c_scheduled_write(channel_task, arg, status)
    try
        socket = arg.socket
        GC.@preserve socket begin
            if status == Int(AWS_TASK_STATUS_RUN_READY)
                n = arg.n
                socket.debug && @info "c_scheduled_write: writing $n bytes"
                data = socket.writebuf.data
                GC.@preserve data begin
                    buf = aws_byte_buf(0, pointer(data), n, C_NULL)
                    bytes_written = 0
                    while bytes_written < n
                        msgptr = aws_channel_acquire_message_from_pool(socket.channel, AWS_IO_MESSAGE_APPLICATION_DATA, n - bytes_written)
                        if msgptr == C_NULL
                            socket.debug && @error "c_scheduled_write: failed to acquire message from pool"
                            close(socket.ch, sockerr("failed to acquire message from pool"))
                            @goto done
                        end
                        msg = StructRef(msgptr)
                        data = Ref(msg.message_data)
                        cap = data[].capacity
                        cursor = Ref(aws_byte_cursor(cap, buf.buffer + bytes_written))
                        GC.@preserve data cursor begin
                            aws_byte_buf_append(data, cursor)
                            msg.message_data = data[]
                        end
                        socket.debug && @info "c_scheduled_write: sending $(data[].len) bytes in message"
                        if aws_channel_slot_send_message(socket.slot, msgptr, AWS_CHANNEL_DIR_WRITE) != 0
                            aws_mem_release(msg.allocator, msgptr)
                            socket.debug && @error "c_scheduled_write: failed to send message"
                            close(socket.ch, sockerr("failed to send message"))
                            @goto done
                        end
                        bytes_written += cap
                    end
                    put!(socket.ch, :write_completed)
                end
            else
                socket.debug && @warn "c_scheduled_write: task cancelled"
                close(socket.ch, sockerr("task cancelled"))
            end
        end
@label done
    finally
        aws_mem_release(default_aws_allocator(), channel_task)
        # arg.socket.debug && @info "c_scheduled_write: write completed"
    end
    return
end

const SCHEDULED_WRITE = Ref{Ptr{Cvoid}}(C_NULL)

mutable struct ScheduledWriteArgs
    socket::Client
    n::Int
end

function Base.write(sock::Client, data)
    @lock sock.writelock begin
        n = write(sock.writebuf, data)
        args = ScheduledWriteArgs(sock, n)
        GC.@preserve args begin
            schedule_channel_task(sock.channel, SCHEDULED_WRITE[], pointer_from_objref(args), "socket channel write")
            take!(sock.ch) # wait for write completion
        end
        skip(sock.writebuf, n) # "consume" the bytes we wrote to our writebuf to reset it for furture writes
        return n
    end
end

Base.flush(sock::Client) = flush(sock.writebuf)

function maybe_increment_read_window(sock::Client, bytes_available)
    available_space = sock.buffer_capacity - bytes_available
    if available_space >= (sock.buffer_capacity ÷ 32)
        sock.debug && @info "Incrementing read window by $available_space bytes"
        aws_channel_slot_increment_read_window(sock.slot, available_space)
    end
end

function Base.read(sock::Client)
    buf = read(sock.readbuf)
    maybe_increment_read_window(sock, bytesavailable(sock.readbuf))
    return buf
end

function Base.read!(sock::Client, buf::Vector{UInt8})
    n = read!(sock.readbuf, buf)
    maybe_increment_read_window(sock, bytesavailable(sock.readbuf))
    return n
end

function Base.read(sock::Client, ::Type{T}) where {T}
    x = read(sock.readbuf, T)
    maybe_increment_read_window(sock, bytesavailable(sock.readbuf))
    return x
end

function Base.read(sock::Client, n::Integer)
    buf = read(sock.readbuf, n)
    maybe_increment_read_window(sock, bytesavailable(sock.readbuf))
    return buf
end

function Base.unsafe_read(sock::Client, ptr::Ptr{UInt8}, n::Integer)
    unsafe_read(sock.readbuf, ptr, n)
    maybe_increment_read_window(sock, bytesavailable(sock.readbuf))
    return
end

function Base.skip(sock::Client, n)
    ret = skip(sock.readbuf, n)
    maybe_increment_read_window(sock, bytesavailable(sock.readbuf))
    return ret
end

Base.bytesavailable(sock::Client) = bytesavailable(sock.readbuf)
Base.eof(sock::Client) = eof(sock.readbuf)
Base.isopen(sock::Client) = sock.slot == C_NULL ? false : aws_socket_is_open(aws_socket_handler_get_socket(FieldRef(sock, :handler)))

function Base.readbytes!(sock::Client, buf::AbstractVector{UInt8}, nb=length(buf))
    act = readbytes!(sock.readbuf, buf, nb)
    maybe_increment_read_window(sock, bytesavailable(sock.readbuf))
    return act
end

function Base.close(sock::Client)
    close(sock.ch)
    close(sock.readbuf)
    close(sock.writebuf)
    if sock.channel != C_NULL
        aws_channel_shutdown(sock.channel, 0)
        sock.channel = C_NULL
    end
    sock.slot = C_NULL
    return
end

function c_on_negotiation_result(handler, slot, error_code, sock)
    GC.@preserve sock begin
        if error_code != 0
            sock.debug && @error "c_on_negotiation_result: error = '$(unsafe_string(aws_error_str(error_code)))'"
            close(sock.ch, sockerr(error_code))
        else
            put!(sock.ch, :negotiated)
        end
    end
    return
end

const ON_NEGOTIATION_RESULT = Ref{Ptr{Cvoid}}(C_NULL)

function c_tls_upgrade(channel_task, arg, status)
    sock = arg.socket
    GC.@preserve sock begin
        if status == Int(AWS_TASK_STATUS_RUN_READY)
            tls_options = FieldRef(arg, :tls_options)
            sock.debug && @info "c_tls_upgrade: initiating tls upgrade"
            slot = aws_channel_slot_new(sock.channel)
            if slot == C_NULL
                close(sock.ch, sockerr("failed to create channel slot for tlsupgrade"))
                @goto done
            end
            channel_handler = aws_tls_client_handler_new(default_aws_allocator(), tls_options, slot)
            if channel_handler == C_NULL
                close(sock.ch, sockerr("failed to create tls client handler"))
                @goto done
            end
            if aws_channel_slot_insert_left(sock.slot, slot) != 0
                close(sock.ch, sockerr("failed to insert channel slot for tlsupgrade"))
                @goto done
            end
            if aws_channel_slot_set_handler(slot, channel_handler) != 0
                close(sock.ch, sockerr("failed to set tls client handler"))
                @goto done
            end
            if aws_tls_client_handler_start_negotiation(channel_handler) != 0
                close(sock.ch, sockerr("failed to start tls negotiation"))
                @goto done
            end
        else
            sock.debug && @warn "c_tls_upgrade: task cancelled"
            close(socket.ch, sockerr("task cancelled"))
            @goto done
        end
@label done
        aws_mem_release(default_aws_allocator(), channel_task)
        sock.debug && @info "c_tls_upgrade: tls upgrade completed"
    end
    return
end

const TLS_UPGRADE = Ref{Ptr{Cvoid}}(C_NULL)

mutable struct TLSUpgradeArgs
    socket::Client
    tls_options::aws_tls_connection_options
end

function tlsupgrade!(sock::Client;
        ssl_cert::Union{String, Nothing}=nothing,
        ssl_key::Union{String, Nothing}=nothing,
        ssl_capath::Union{String, Nothing}=nothing,
        ssl_cacert::Union{String, Nothing}=nothing,
        ssl_insecure::Bool=false,
        ssl_alpn_list::Union{String, Nothing}=nothing
    )
    tls_options = LibAwsIO.tlsoptions(
        sock.host;
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        ssl_capath=ssl_capath,
        ssl_cacert=ssl_cacert,
        ssl_insecure=ssl_insecure,
        ssl_alpn_list=ssl_alpn_list,
        on_negotiation_result=ON_NEGOTIATION_RESULT[],
        on_negotiation_result_user_data=sock
    )
    arg = TLSUpgradeArgs(sock, tls_options)
    GC.@preserve arg begin
        schedule_channel_task(sock.channel, TLS_UPGRADE[], pointer_from_objref(arg), "socket channel tls upgrade")
        take!(sock.ch) # wait for tls upgrade completion
    end
    return
end
