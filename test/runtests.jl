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

println("testing tls")
sock = Sockets.Client("www.google.com", 443; tls=true, ssl_alpn_list="http/1.1")
write(sock, "GET / HTTP/1.1\r\nHost: www.google.com\r\n\r\n")
sleep(0.1)
data = String(read(sock, 100))
@test startswith(data, "HTTP/1.1 200 OK")
close(sock)

println("testing tls upgrade")
sock = Sockets.Client("www.google.com", 443; tls=false, debug=true)
AwsIO.Sockets.tlsupgrade!(sock; ssl_alpn_list="http/1.1")
write(sock, "GET / HTTP/1.1\r\nHost: www.google.com\r\n\r\n")
sleep(0.1)
data = String(read(sock, 100))
@test startswith(data, "HTTP/1.1 200 OK")
close(sock)

end
