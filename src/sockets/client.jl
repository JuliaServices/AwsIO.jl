_id(obj) = string(objectid(obj); base=58)

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

# 256KB
const DEFAULT_READ_BUFFER_SIZE = 256 * 1024

mutable struct Client <: IO
    host::String
    port::Int
    debug::Bool
    tls::Bool
    socket_options::aws_socket_options
    tls_options::Union{aws_tls_connection_options, Nothing}
    tls_handler::Ptr{aws_channel_handler} # only used in tlsupgrade!
    buffer_capacity::Int
    channel::Ptr{aws_channel}
    slot::Ptr{aws_channel_slot}
    ch::Channel{Symbol}
    readbuf::Base.BufferStream
    # keep track of our own window_size that corresponds to the channel slot.window_size (but it's more internal to aws-c-io)
    # we want it to be equal to the amount of space left in our readbuf; buffer_capacity - bytesavailable(readbuf)
    window_size::Int
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
        x = new(host, port, debug, tls, socket_options, tls_options, C_NULL, buffer_capacity, C_NULL, C_NULL, Channel{Symbol}(0), Base.BufferStream(), buffer_capacity, ReentrantLock(), PipeBuffer())
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
            finalizer(close, x)
            return x
        end
    end
end

mutable struct IncrementReadWindowArgs
    socket::Client
    increment::Int
end

function c_increment_read_window_task(channel_task, arg, status)
    try
        socket = arg.socket
        if status == Int(AWS_TASK_STATUS_RUN_READY)
            socket.window_size += arg.increment
            slotobj = unsafe_load(socket.slot)
            # TODO: check return value of aws_channel_slot_increment_read_window
            aws_channel_slot_increment_read_window(socket.slot, arg.increment)
            put!(socket.ch, :read_window_incremented)
            socket.debug && @info "[$(_id(socket))]: c_increment_read_window_task: incremented read window by $(arg.increment) bytes"
        else
            sock.debug && @warn "c_increment_read_window_task: task cancelled"
            close(socket.ch, sockerr("task cancelled"))
        end
    catch e
        close(arg.socket.ch, sockerr(e))
    finally
        aws_mem_release(default_aws_allocator(), channel_task)
    end
    return
end

const INCREMENT_READ_WINDOW_TASK = Ref{Ptr{Cvoid}}()

function check_increment_read_window!(socket::Client)
    slotobj = unsafe_load(socket.slot)
    # we want our window_size to be equal to the amount of space left in our readbuf
    desired_size = socket.buffer_capacity - bytesavailable(socket.readbuf)
    increment = desired_size - socket.window_size
    socket.debug && @warn "[$(_id(socket))]: check_increment_read_window: socket.window_size = $(socket.window_size), slot.window_size = $(slotobj.window_size), bytesavailable = $(bytesavailable(socket.readbuf)), desired_size = $desired_size, increment = $increment"
    if increment > 0
        arg = IncrementReadWindowArgs(socket, increment)
        GC.@preserve arg begin
            schedule_channel_task(socket.channel, INCREMENT_READ_WINDOW_TASK[], pointer_from_objref(arg), "socket channel increment read window")
            take!(socket.ch) # wait for read window increment
        end
    end
    return
end

function c_process_read_message(handler, slot, messageptr)::Cint
    msg = StructRef(messageptr)
    data = msg.message_data
    sock = unsafe_pointer_to_objref(handler.impl)
    slotobj = unsafe_load(slot)
    ret = AWS_OP_ERR
    try
        unsafe_write(sock.readbuf, data.buffer, data.len)
        ret = AWS_OP_SUCCESS
        aws_mem_release(msg.allocator, messageptr)
        sock.window_size -= Int(data.len)
        sock.debug && @info "[$(_id(sock))]: c_process_read_message: read $(data.len) bytes, sock.window_size = $(sock.window_size), slot.window_size = $(slotobj.window_size)"
    catch e
        close(sock.ch, sockerr(e))
    end
    return ret
end

function c_process_write_message(handler, slot, messageptr)::Cint
    # this should never be called since we only want to be the last slot in the channel
    return AWS_OP_ERR
end

function c_increment_read_window(handler, slot, size)::Cint
    sock = unsafe_pointer_to_objref(handler.impl)
    slotobj = unsafe_load(slot)
    sock.debug && @info "[$(_id(sock))]: c_increment_read_window: size = $size, slot.window_size = $(slotobj.window_size), current_window_update_batch_size = $(slotobj.current_window_update_batch_size)"
    # aws_channel_slot_increment_read_window(slot, size)
    return AWS_OP_SUCCESS
end

function c_shutdown(handler, slot, dir, error_code, free_scarce_resources_immediately)::Cint
    sock = unsafe_pointer_to_objref(handler.impl)
    close(sock.ch, sockerr(error_code))
    return aws_channel_slot_on_handler_shutdown_complete(slot, dir, error_code, free_scarce_resources_immediately)
end

function c_initial_window_size(handler)::Csize_t
    sock = unsafe_pointer_to_objref(handler.impl)
    # Return the buffer capacity as the initial window size
    sock.debug && @info "[$(_id(sock))]: c_initial_window_size: $(sock.buffer_capacity) bytes"
    return sock.buffer_capacity
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
    if error_code != 0
        socket.debug && @error "c_setup_callback: error = '$(unsafe_string(aws_error_str(error_code)))'"
        close(socket.ch, sockerr(error_code))
    else
        slot = aws_channel_slot_new(channel)
        if slot == C_NULL
            socket.debug && @error "c_setup_callback: failed to create channel slot"
            close(socket.ch, sockerr("failed to create channel slot"))
            return
        end
        if aws_channel_slot_insert_end(channel, slot) != 0
            aws_channel_slot_remove(slot)
            socket.debug && @error "c_setup_callback: failed to insert channel slot"
            close(socket.ch, sockerr("failed to insert channel slot"))
            return
        end
        socket.handler = aws_channel_handler(Base.unsafe_convert(Ptr{aws_channel_handler_vtable}, RW_HANDLER_VTABLE), default_aws_allocator(), C_NULL, pointer_from_objref(socket))
        if aws_channel_slot_set_handler(slot, FieldRef(socket, :handler)) != 0
            aws_channel_slot_remove(slot)
            socket.debug && @error "c_setup_callback: failed to set channel slot handler"
            close(socket.ch, sockerr("failed to set channel slot handler"))
            return
        end
        socket.channel = channel
        socket.slot = slot
        try
            put!(socket.ch, :setup)
        catch e
            close(socket.ch, sockerr(e))
        end
    end
    return
end

const SETUP_CALLBACK = Ref{Ptr{Cvoid}}(C_NULL)

function c_shutdown_callback(bootstrap, error_code, channel, socket)
    socket.debug && @warn "c_shutdown_callback"
    close(socket.ch)
    close(socket.readbuf)
    close(socket.writebuf)
    socket.channel = C_NULL
    socket.slot = C_NULL
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
                socket.debug && @info "[$(_id(socket))]: c_scheduled_write: writing $n bytes"
                writebufdata = socket.writebuf.data
                GC.@preserve writebufdata begin
                    buf = aws_byte_buf(0, pointer(writebufdata), n, C_NULL)
                    bytes_written = 0
                    while bytes_written < n
                        msgptr = aws_channel_acquire_message_from_pool(socket.channel, AWS_IO_MESSAGE_APPLICATION_DATA, n - bytes_written)
                        msg = StructRef(msgptr)
                        data = Ref(msg.message_data)
                        cap = data[].capacity
                        cursor = Ref(aws_byte_cursor(cap, buf.buffer + bytes_written))
                        GC.@preserve data cursor begin
                            aws_byte_buf_append(data, cursor)
                            msg.message_data = data[]
                        end
                        socket.debug && @info "[$(_id(socket))]: c_scheduled_write: sending $(data[].len) bytes in message: $(String(writebufdata[1:min(length(writebufdata), 40)]))..."
                        if aws_channel_slot_send_message(socket.slot, msgptr, AWS_CHANNEL_DIR_WRITE) != 0
                            aws_mem_release(msg.allocator, msgptr)
                            socket.debug && @error "c_scheduled_write: failed to send message"
                            close(socket.ch, sockerr("failed to send message"))
                            @goto done
                        end
                        bytes_written += cap
                    end
                    try
                        put!(socket.ch, :write_completed)
                    catch e
                        close(socket.ch, sockerr(e))
                    end
                end
            else
                socket.debug && @warn "c_scheduled_write: task cancelled"
                close(socket.ch, sockerr("task cancelled"))
            end
        end
@label done
    finally
        aws_mem_release(default_aws_allocator(), channel_task)
        # arg.socket.debug && @info #"[$(_id())]: c_scheduled_write: write completed"
    end
    return
end

const SCHEDULED_WRITE = Ref{Ptr{Cvoid}}(C_NULL)

mutable struct ScheduledWriteArgs
    socket::Client
    n::Int
end

function Base.unsafe_write(sock::Client, ref::Ptr{UInt8}, nbytes::UInt)
    @lock sock.writelock begin
        Base.unsafe_write(sock.writebuf, ref, nbytes)
        args = ScheduledWriteArgs(sock, nbytes)
        GC.@preserve args begin
            schedule_channel_task(sock.channel, SCHEDULED_WRITE[], pointer_from_objref(args), "socket channel write")
            take!(sock.ch) # wait for write completion
        end
        skip(sock.writebuf, nbytes) # "consume" the bytes we wrote to our writebuf to reset it for furture writes
        return nbytes
    end
end

Base.flush(sock::Client) = flush(sock.writebuf)

function Base.unsafe_read(sock::Client, ptr::Ptr{UInt8}, n::UInt64)
    unsafe_read(sock.readbuf, ptr, n)
    check_increment_read_window!(sock)
    return
end

function Base.read(sock::Client, ::Type{UInt8})
    ret = read(sock.readbuf, UInt8)
    check_increment_read_window!(sock)
    return ret
end

function Base.readbytes!(sock::Client, buf::AbstractVector{UInt8}, n::Integer=length(buf))
    readbytes!(sock.readbuf, buf, n)
    check_increment_read_window!(sock)
    return n
end

function Base.read(sock::Client, n::Integer)
    buf = read(sock.readbuf, n)
    check_increment_read_window!(sock)
    return buf
end

function Base.skip(sock::Client, n)
    ret = skip(sock.readbuf, n)
    check_increment_read_window!(sock)
    return ret
end

Base.bytesavailable(sock::Client) = bytesavailable(sock.readbuf)
Base.eof(sock::Client) = eof(sock.readbuf)

Base.isopen(sock::Client) = sock.slot == C_NULL ? false : aws_socket_is_open(aws_socket_handler_get_socket(FieldRef(sock, :handler)))

function Base.close(sock::Client)
    close(sock.ch)
    close(sock.readbuf)
    close(sock.writebuf)
    if sock.channel != C_NULL
        aws_channel_shutdown(sock.channel, 0)
        sock.channel = C_NULL
    end
    if sock.tls_options !== nothing
        ref = Ref(sock.tls_options)
        GC.@preserve ref begin
            aws_tls_connection_options_clean_up(ref)
        end
        sock.tls_options = nothing
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
            try
                put!(sock.ch, :negotiated)
            catch e
                close(sock.ch, sockerr(e))
            end
        end
    end
    return
end

const ON_NEGOTIATION_RESULT = Ref{Ptr{Cvoid}}(C_NULL)

function c_tls_upgrade(channel_task, arg, status)
    sock = arg.socket
    if status == Int(AWS_TASK_STATUS_RUN_READY)
        tls_options = FieldRef(arg, :tls_options)
        sock.debug && @info "[$(_id(sock))]: c_tls_upgrade: initiating tls upgrade"
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
        sock.tls_handler = channel_handler
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
        sock.tls_options = arg.tls_options
    else
        sock.debug && @warn "c_tls_upgrade: task cancelled"
        close(socket.ch, sockerr("task cancelled"))
        @goto done
    end
@label done
    aws_mem_release(default_aws_allocator(), channel_task)
    sock.debug && @info "[$(_id(sock))]: c_tls_upgrade: tls upgrade completed"
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
