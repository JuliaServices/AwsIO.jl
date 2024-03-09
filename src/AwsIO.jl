module AwsIO

using Logging

import AwsC: AwsC, SUCCESS, ERROR, mem_acquire, mem_release, error_str, byte_cursor, byte_buf, append

include("LibAwsIO.jl")
import .LibAwsIO: LibAwsIO, aws_app_client_handler_new, aws_app_client_handler_write,
    aws_client_bootstrap_new_socket_channel, aws_channel_setup_client_tls, tlsoptions,
    aws_channel_shutdown, aws_channel_get_first_slot, aws_channel, aws_channel_slot,
    aws_channel_handler, aws_io_message, aws_socket_channel_bootstrap_options, aws_byte_buf,
    aws_client_bootstrap, aws_app_client_handler_tls_upgrade

struct SocketError <: Exception
    msg::String
end

mutable struct Socket
    debug::Bool
    tls::Bool
    channel::Ptr{aws_channel}
    handler::Ptr{Cvoid}
    ch::Channel{Symbol}
    readbuf::Base.BufferStream
    writelock::ReentrantLock
    writebuf::IOBuffer
    options::aws_socket_channel_bootstrap_options

    function Socket(host, port; tls::Bool=false, debug::Bool=false, kw...)
        x = new(debug, tls, C_NULL, C_NULL, Channel{Symbol}(0), Base.BufferStream(), ReentrantLock(), PipeBuffer())
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

function c_on_read(handler, buffer::Ptr{aws_byte_buf}, socket::Socket)
    buf = unsafe_load(buffer)
    try
        unsafe_write(socket.readbuf, buf.buffer, buf.len)
    catch e
        close(socket.ch, e)
    end
    return
end

const ON_READ = Ref{Ptr{Cvoid}}(C_NULL)

function c_setup_callback(bootstrap, error_code, channel, socket)
    if error_code != 0
        socket.debug && @error "c_setup_callback: error = '$(error_str(error_code))'"
        close(socket.ch, SocketError(error_str(error_code)))
    else
        socket.handler = aws_app_client_handler_new(AwsC.allocator(), channel, ON_READ[], socket)
        socket.channel = channel
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
    socket.handler = C_NULL
    return
end

const SHUTDOWN_CALLBACK = Ref{Ptr{Cvoid}}(C_NULL)

function c_on_message_write_completed(channel, message, err_code, socket::Socket)
    if err_code != 0
        socket.debug && @error "c_on_message_write_completed: error = '$(error_str(err_code))'"
        close(socket.ch, SocketError(error_str(err_code)))
    else
        put!(socket.ch, :write_completed)
    end
    return
end

const ON_MESSAGE_WRITE_COMPLETED = Ref{Ptr{Cvoid}}(C_NULL)

function Base.write(sock::Socket, data)
    @lock sock.writelock begin
        n = write(sock.writebuf, data)
        n == 0 && return 0
        buf = byte_buf(pointer(sock.writebuf.data), n)
        aws_app_client_handler_write(sock.handler, buf, ON_MESSAGE_WRITE_COMPLETED[], sock)
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
    sock.handler = C_NULL
    return
end

function c_on_negotiation_result(handler::Ptr{Cvoid}, slot::Ptr{aws_channel_slot}, error_code::Cint, sock::Socket)
    if error_code != 0
        # sock.debug && @error "c_on_negotiation_result: error = '$(error_str(error_code))'"
        close(sock.ch, SocketError(error_str(error_code)))
    else
        put!(sock.ch, :negotiated)
    end
    return
end

const ON_NEGOTIATION_RESULT = Ref{Ptr{Cvoid}}(C_NULL)

function tlsupgrade!(sock::Socket;
        ssl_cert::Union{String, Nothing}=nothing,
        ssl_key::Union{String, Nothing}=nothing,
        ssl_capath::Union{String, Nothing}=nothing,
        ssl_cacert::Union{String, Nothing}=nothing,
        ssl_insecure::Bool=false,
        ssl_alpn_list::Union{String, Nothing}=nothing
    )
    if aws_app_client_handler_tls_upgrade(AwsC.allocator(), sock.channel, sock.options.host, ON_NEGOTIATION_RESULT[], sock) != 0
        throw(SocketError("failed to upgrade to tls"))
    end
    take!(sock.ch) # wait for tls negotiation
    return
end

function __init__()
    SETUP_CALLBACK[] = @cfunction(c_setup_callback, Cvoid, (Ptr{aws_client_bootstrap}, Cint, Ptr{aws_channel}, Any))
    SHUTDOWN_CALLBACK[] = @cfunction(c_shutdown_callback, Cvoid, (Ptr{aws_client_bootstrap}, Cint, Ptr{aws_channel}, Any))
    ON_NEGOTIATION_RESULT[] = @cfunction(c_on_negotiation_result, Cvoid, (Ptr{Cvoid}, Ptr{aws_channel_slot}, Cint, Any))
    ON_READ[] = @cfunction(c_on_read, Cvoid, (Ptr{Cvoid}, Ptr{aws_byte_buf}, Any))
    ON_MESSAGE_WRITE_COMPLETED[] = @cfunction(c_on_message_write_completed, Cvoid, (Ptr{aws_channel}, Ptr{aws_io_message}, Cint, Any))
    return
end

end