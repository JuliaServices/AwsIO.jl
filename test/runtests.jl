using Test, AwsIO

import AwsIO: Sockets

@testset "AwsIO" begin

    println("testing non-tls")
    sock = Sockets.Client("www.google.com", 80)
    write(sock, "GET / HTTP/1.1\r\nHost: www.google.com\r\n\r\n")
    sleep(0.1)
    data = String(read(sock, 100))
    @test startswith(data, "HTTP/1.1 200 OK")
    close(sock)

    println("tracing memory")
    AwsIO.Sockets.trace_memory!()
    println("testing tls")
    sock = Sockets.Client("www.google.com", 443; tls=true, ssl_alpn_list="http/1.1")
    write(sock, "GET / HTTP/1.1\r\nHost: www.google.com\r\n\r\n")
    sleep(0.1)
    data = String(read(sock, 100))
    @test startswith(data, "HTTP/1.1 200 OK")
    close(sock)
    Sockets.set_log_level!(1)
    AwsIO.Sockets.trace_memory_dump()

    println("testing tls upgrade")
    sock = Sockets.Client("www.google.com", 443; tls=false, buffer_capacity=2^12, enable_read_back_pressure=true)
    AwsIO.Sockets.tlsupgrade!(sock; ssl_alpn_list="http/1.1")
    write(sock, "GET / HTTP/1.1\r\nHost: www.google.com\r\n\r\n")
    sleep(0.1)
    data = String(read(sock, 1000))
    @test startswith(data, "HTTP/1.1 200 OK")
    data = String(read(sock, 1000))
    data = String(read(sock, 1000))
    data = String(read(sock, 1000))
    data = String(read(sock, 1000))
    close(sock)
    AwsIO.Sockets.trace_memory_dump()

end
