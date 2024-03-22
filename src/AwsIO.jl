module AwsIO

using Logging

import AwsC: AwsC, SUCCESS, ERROR, mem_acquire, mem_release, error_str, byte_cursor, byte_buf, append

include("LibAwsIO.jl")
import .LibAwsIO: LibAwsIO, get_allocator, get_message_data, set_message_data!, aws_channel_slot_increment_read_window,
    aws_channel_slot_on_handler_shutdown_complete, aws_channel_handler_vtable, aws_channel_slot_new,
    aws_channel_slot_insert_end, aws_channel_slot_set_handler, aws_channel_handler, aws_channel,
    aws_channel_slot, AWS_TASK_STATUS_RUN_READY, AWS_CHANNEL_DIR_WRITE, aws_channel_acquire_message_from_pool,
    aws_channel_slot_send_message, aws_client_bootstrap, aws_socket_channel_bootstrap_options, aws_channel_task,
    aws_io_message, aws_client_bootstrap_new_socket_channel, aws_channel_task_init, aws_channel_schedule_task_now,
    tlsoptions, aws_tls_client_handler_new, aws_tls_client_handler_start_negotiation,
    aws_channel_slot_insert_left, aws_channel_shutdown, aws_tls_connection_options_clean_up

struct SocketError <: Exception
    msg::String
end

sockerr(msg::String) = CapturedException(SocketError(msg), Base.backtrace())
sockerr(e::Exception) = CapturedException(e, Base.backtrace())

function c_process_read_message(handler, slot, messageptr)::Cint
    data = get_message_data(messageptr)
    handler.impl.debug && @info "c_process_read_message: $(data.len) bytes"
    ret = ERROR
    try
        unsafe_write(handler.impl.readbuf, data.buffer, data.len)
        ret = SUCCESS
        mem_release(get_allocator(messageptr), messageptr)
    catch e
        close(handler.impl.ch, sockerr(e))
    end
    return ret
end

function c_process_write_message(handler, slot, messageptr)::Cint
    handler.impl.debug && @info "c_process_write_message"
    # this should never be called since we only want to be the last slot in the channel
    return ERROR
end

function c_increment_read_window(handler, slot, size)::Cint
    handler.impl.debug && @info "c_increment_read_window"
    aws_channel_slot_increment_read_window(slot, size)
    return SUCCESS
end

function c_shutdown(handler, slot, dir, error_code, free_scarce_resources_immediately)::Cint
    handler.impl.debug && @warn "c_shutdown: dir = $dir"
    close(handler.impl.ch, sockerr(error_str(error_code)))
    return aws_channel_slot_on_handler_shutdown_complete(slot, dir, error_code, free_scarce_resources_immediately)
end

function c_initial_window_size(channel_handler)::Csize_t
    return typemax(Int64)
end

function c_message_overhead(channel_handler)::Csize_t
    return 0
end

function c_destroy(channel_handler)
    return
end

function c_reset_statistics(channel_handler)::Cvoid
    return SUCCESS
end

function c_gather_statistics(channel_handler, stats#=::Ptr{aws_array_list}=#)::Cvoid
    return SUCCESS
end

function c_trigger_read(channel_handler)::Cvoid
    return SUCCESS
end

const RW_HANDLER_VTABLE = Ref{aws_channel_handler_vtable}()

function c_setup_callback(bootstrap, error_code, channel, socket)
    if error_code != 0
        socket.debug && @error "c_setup_callback: error = '$(error_str(error_code))'"
        close(socket.ch, sockerr(error_str(error_code)))
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
        handler = aws_channel_handler(RW_HANDLER_VTABLE[], AwsC.allocator(), C_NULL, socket)
        if aws_channel_slot_set_handler(slot, handler) != 0
            socket.debug && @error "c_setup_callback: failed to set channel slot handler"
            close(socket.ch, sockerr("failed to set channel slot handler"))
        end
        socket.channel = channel
        socket.slot = slot
        socket.handler = handler
        put!(socket.ch, :setup)
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
    socket.handler = nothing
    return
end

const SHUTDOWN_CALLBACK = Ref{Ptr{Cvoid}}(C_NULL)

mutable struct Socket
    debug::Bool
    tls::Bool
    channel::Ptr{aws_channel}
    slot::Ptr{aws_channel_slot}
    handler::Union{aws_channel_handler, Nothing}
    ch::Channel{Symbol}
    readbuf::Base.BufferStream
    writelock::ReentrantLock
    writebuf::IOBuffer
    options::aws_socket_channel_bootstrap_options

    function Socket(host, port; tls::Bool=false, debug::Bool=false, kw...)
        x = new(debug, tls, C_NULL, C_NULL, nothing, Channel{Symbol}(0), Base.BufferStream(), ReentrantLock(), PipeBuffer())
        x.options = aws_socket_channel_bootstrap_options(
            host,
            port;
            tls,
            user_data=x,
            setup_callback=SETUP_CALLBACK[],
            shutdown_callback=SHUTDOWN_CALLBACK[],
            kw...)
        aws_client_bootstrap_new_socket_channel(x.options) != 0 && throw(SocketError("failed to create socket"))
        take!(x.ch) # wait for connection
        return x
    end
end

function schedule_channel_task(channel, task_fn, arg, type_tag)
    task = aws_channel_task_init(task_fn, arg, type_tag)
    aws_channel_schedule_task_now(channel, task)
end

function c_scheduled_write(channel_task, (socket, n), status)
    if status == Int(AWS_TASK_STATUS_RUN_READY)
        socket.debug && @info "c_scheduled_write: writing $n bytes"
        buf = byte_buf(n, pointer(socket.writebuf.data))
        bytes_written = 0
        while bytes_written < n
            msgptr = aws_channel_acquire_message_from_pool(socket.channel, n - bytes_written)
            if msgptr == C_NULL
                socket.debug && @error "c_scheduled_write: failed to acquire message from pool"
                close(socket.ch, sockerr("failed to acquire message from pool"))
                @goto done
            end
            data = get_message_data(msgptr)
            cap = data.capacity
            cursor = byte_cursor(cap, buf.buffer + bytes_written)
            data = append(data, cursor)
            set_message_data!(msgptr, data)
            socket.debug && @info "c_scheduled_write: sending $(data.len) bytes in message"
            if aws_channel_slot_send_message(socket.slot, msgptr, AWS_CHANNEL_DIR_WRITE) != 0
                mem_release(AwsC.allocator(), msgptr)
                socket.debug && @error "c_scheduled_write: failed to send message"
                close(socket.ch, sockerr("failed to send message"))
                @goto done
            end
            bytes_written += cap
        end
    else
        socket.debug && @warn "c_scheduled_write: task cancelled"
        close(socket.ch, sockerr("task cancelled"))
        @goto done
    end
    put!(socket.ch, :write_completed)
@label done
    mem_release(AwsC.allocator(), channel_task)
    socket.debug && @info "c_scheduled_write: write completed"
    return
end

const SCHEDULED_WRITE = Ref{Ptr{Cvoid}}(C_NULL)

function Base.write(sock::Socket, data)
    @lock sock.writelock begin
        n = write(sock.writebuf, data)
        schedule_channel_task(sock.channel, SCHEDULED_WRITE[], (sock, n), "socket channel write")
        take!(sock.ch) # wait for write completion
        skip(sock.writebuf, n) # "consume" the bytes we wrote to our writebuf to reset it for furture writes
        return n
    end
end

Base.flush(sock::Socket) = flush(sock.writebuf)

Base.read(sock::Socket) = read(sock.readbuf)
Base.read!(sock::Socket, buf::Vector{UInt8}) = read!(sock.readbuf, buf)
Base.read(sock::Socket, ::Type{T}) where {T} = read(sock.readbuf, T)
Base.read(sock::Socket, n::Integer) = read(sock.readbuf, n)
Base.unsafe_read(sock::Socket, ptr::Ptr{UInt8}, n::Integer) = unsafe_read(sock.readbuf, ptr, n)

Base.skip(sock::Socket, n) = skip(sock.readbuf, n)

Base.isopen(sock::Socket) = isopen(sock.ch)

function Base.close(sock::Socket)
    close(sock.ch)
    close(sock.readbuf)
    close(sock.writebuf)
    if sock.channel != C_NULL
        aws_channel_shutdown(sock.channel, 0)
        sock.channel = C_NULL
    end
    sock.slot = C_NULL
    sock.handler = nothing
    return
end

function c_on_negotiation_result(handler, slot, error_code, sock)
    if error_code != 0
        sock.debug && @error "c_on_negotiation_result: error = '$(error_str(error_code))'"
        close(sock.ch, sockerr(error_str(error_code)))
    else
        put!(sock.ch, :negotiated)
    end
    return
end

const ON_NEGOTIATION_RESULT = Ref{Ptr{Cvoid}}(C_NULL)

function c_tls_upgrade(channel_task, (sock, tls_options), status)
    if status == Int(AWS_TASK_STATUS_RUN_READY)
        sock.debug && @info "c_tls_upgrade: initiating tls upgrade"
        slot = aws_channel_slot_new(sock.channel)
        if slot == C_NULL
            close(sock.ch, sockerr("failed to create channel slot for tlsupgrade"))
            @goto done
        end
        channel_handler = aws_tls_client_handler_new(AwsC.allocator(), tls_options, slot)
        if channel_handler == C_NULL
            close(sock.ch, sockerr("failed to create tls client handler"))
            @goto done
        end
        # options are copied in aws_tls_client_handler_new, so we can free them now
        # aws_tls_connection_options_clean_up(tls_options)
        # mem_release(AwsC.allocator(), tls_options)
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
    mem_release(AwsC.allocator(), channel_task)
    sock.debug && @info "c_tls_upgrade: tls upgrade completed"
    return
end

const TLS_UPGRADE = Ref{Ptr{Cvoid}}(C_NULL)

function tlsupgrade!(sock::Socket;
        ssl_cert::Union{String, Nothing}=nothing,
        ssl_key::Union{String, Nothing}=nothing,
        ssl_capath::Union{String, Nothing}=nothing,
        ssl_cacert::Union{String, Nothing}=nothing,
        ssl_insecure::Bool=false,
        ssl_alpn_list::Union{String, Nothing}=nothing
    )
    tls_options = tlsoptions(
        sock.options.host;
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        ssl_capath=ssl_capath,
        ssl_cacert=ssl_cacert,
        ssl_insecure=ssl_insecure,
        ssl_alpn_list=ssl_alpn_list,
        on_negotiation_result=ON_NEGOTIATION_RESULT[],
        user_data=sock
    )
    schedule_channel_task(sock.channel, TLS_UPGRADE[], (sock, tls_options), "socket channel tls upgrade")
    take!(sock.ch) # wait for tls upgrade completion
    return
end

function __init__()
    SETUP_CALLBACK[] = @cfunction(c_setup_callback, Cvoid, (Ptr{aws_client_bootstrap}, Cint, Ptr{aws_channel}, Any))
    SHUTDOWN_CALLBACK[] = @cfunction(c_shutdown_callback, Cvoid, (Ptr{aws_client_bootstrap}, Cint, Ptr{aws_channel}, Any))
    SCHEDULED_WRITE[] = @cfunction(c_scheduled_write, Cvoid, (Ptr{aws_channel_task}, Any, Cint))
    RW_HANDLER_VTABLE[] = aws_channel_handler_vtable(
        @cfunction(c_process_read_message, Cint, (Ref{aws_channel_handler}, Ptr{aws_channel_slot}, Ptr{aws_io_message})),
        @cfunction(c_process_write_message, Cint, (Ref{aws_channel_handler}, Ptr{aws_channel_slot}, Ptr{aws_io_message})),
        @cfunction(c_increment_read_window, Cint, (Ref{aws_channel_handler}, Ptr{aws_channel_slot}, Csize_t)),
        @cfunction(c_shutdown, Cint, (Ref{aws_channel_handler}, Ptr{aws_channel_slot}, Cint, Cint, Bool)),
        @cfunction(c_initial_window_size, Csize_t, (Ref{aws_channel_handler},)),
        @cfunction(c_message_overhead, Csize_t, (Ref{aws_channel_handler},)),
        @cfunction(c_destroy, Cvoid, (Ref{aws_channel_handler},))
    )
    ON_NEGOTIATION_RESULT[] = @cfunction(c_on_negotiation_result, Cvoid, (Ptr{Cvoid}, Ptr{aws_channel_slot}, Cint, Any))
    TLS_UPGRADE[] = @cfunction(c_tls_upgrade, Cvoid, (Ptr{aws_channel_task}, Any, Cint))
    return
end

end