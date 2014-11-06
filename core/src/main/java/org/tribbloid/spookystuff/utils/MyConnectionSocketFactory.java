package org.tribbloid.spookystuff.utils;

import org.apache.http.HttpHost;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.protocol.HttpContext;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketTimeoutException;

//public class MyConnectionSocketFactory implements ConnectionSocketFactory {
//
//    @Override
//    public Socket createSocket(final HttpContext context) throws IOException {
//        InetSocketAddress socksaddr = (InetSocketAddress) context.getAttribute("socks.address");
//        Proxy proxy = new Proxy(Proxy.Type.SOCKS, socksaddr);
//        return new Socket(proxy);
//    }
//
//    @Override
//    public Socket connectSocket(
//            final int connectTimeout,
//            final Socket socket,
//            final HttpHost host,
//            final InetSocketAddress remoteAddress,
//            final InetSocketAddress localAddress,
//            final HttpContext context) throws IOException, ConnectTimeoutException {
//        Socket sock;
//        if (socket != null) {
//            sock = socket;
//        } else {
//            sock = createSocket(context);
//        }
//        if (localAddress != null) {
//            sock.bind(localAddress);
//        }
//        try {
//            sock.connect(remoteAddress, connectTimeout);
//        } catch (SocketTimeoutException ex) {
//            throw new ConnectTimeoutException(ex, host, remoteAddress.getAddress());
//        }
//        return sock;
//    }
//}

public class MyConnectionSocketFactory extends SSLConnectionSocketFactory {

    public MyConnectionSocketFactory(final SSLContext sslContext) {
        super(sslContext);
    }

    @Override
    public Socket createSocket(final HttpContext context) throws IOException {
        InetSocketAddress socksaddr = (InetSocketAddress) context.getAttribute("socks.address");
        Proxy proxy = new Proxy(Proxy.Type.SOCKS, socksaddr);
        return new Socket(proxy);
    }

}