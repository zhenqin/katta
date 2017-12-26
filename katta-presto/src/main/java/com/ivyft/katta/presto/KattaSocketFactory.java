package com.ivyft.katta.presto;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 2017/12/26
 * Time: 13:48
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaSocketFactory extends SocketFactory {


    public KattaSocketFactory() {

    }


    @Override
    public Socket createSocket() throws IOException {
        return new Socket();
    }

    /***
     * Creates a Socket connected to the given host and port.
     * <p>
     * @param host The hostname to connect to.
     * @param port The port to connect to.
     * @return A Socket connected to the given host and port.
     * @exception UnknownHostException  If the hostname cannot be resolved.
     * @exception IOException If an I/O error occurs while creating the Socket.
     ***/
    @Override
    public Socket createSocket(String host, int port)
            throws UnknownHostException, IOException {
        return new Socket(host, port);
    }

    /***
     * Creates a Socket connected to the given host and port.
     * <p>
     * @param address The address of the host to connect to.
     * @param port The port to connect to.
     * @return A Socket connected to the given host and port.
     * @exception IOException If an I/O error occurs while creating the Socket.
     ***/
    @Override
    public Socket createSocket(InetAddress address, int port)
            throws IOException {
        return new Socket(address, port);
    }

    /***
     * Creates a Socket connected to the given host and port and
     * originating from the specified local address and port.
     * <p>
     * @param host The hostname to connect to.
     * @param port The port to connect to.
     * @param localAddr  The local address to use.
     * @param localPort  The local port to use.
     * @return A Socket connected to the given host and port.
     * @exception UnknownHostException  If the hostname cannot be resolved.
     * @exception IOException If an I/O error occurs while creating the Socket.
     ***/
    @Override
    public Socket createSocket(String host, int port,
                               InetAddress localAddr, int localPort)
            throws UnknownHostException, IOException {
        return new Socket(host, port, localAddr, localPort);
    }

    /***
     * Creates a Socket connected to the given host and port and
     * originating from the specified local address and port.
     * <p>
     * @param address The address of the host to connect to.
     * @param port The port to connect to.
     * @param localAddr  The local address to use.
     * @param localPort  The local port to use.
     * @return A Socket connected to the given host and port.
     * @exception IOException If an I/O error occurs while creating the Socket.
     ***/
    @Override
    public Socket createSocket(InetAddress address, int port,
                               InetAddress localAddr, int localPort)
            throws IOException {
        return new Socket(address, port, localAddr, localPort);
    }

    /***
     * Creates a ServerSocket bound to a specified port.  A port
     * of 0 will create the ServerSocket on a system-determined free port.
     * <p>
     * @param port  The port on which to listen, or 0 to use any free port.
     * @return A ServerSocket that will listen on a specified port.
     * @exception IOException If an I/O error occurs while creating
     *                        the ServerSocket.
     ***/
    public ServerSocket createServerSocket(int port) throws IOException {
        return new ServerSocket(port);
    }

    /***
     * Creates a ServerSocket bound to a specified port with a given
     * maximum queue length for incoming connections.  A port of 0 will
     * create the ServerSocket on a system-determined free port.
     * <p>
     * @param port  The port on which to listen, or 0 to use any free port.
     * @param backlog  The maximum length of the queue for incoming connections.
     * @return A ServerSocket that will listen on a specified port.
     * @exception IOException If an I/O error occurs while creating
     *                        the ServerSocket.
     ***/
    public ServerSocket createServerSocket(int port, int backlog)
            throws IOException {
        return new ServerSocket(port, backlog);
    }

    /***
     * Creates a ServerSocket bound to a specified port on a given local
     * address with a given maximum queue length for incoming connections.
     * A port of 0 will
     * create the ServerSocket on a system-determined free port.
     * <p>
     * @param port  The port on which to listen, or 0 to use any free port.
     * @param backlog  The maximum length of the queue for incoming connections.
     * @param bindAddr  The local address to which the ServerSocket should bind.
     * @return A ServerSocket that will listen on a specified port.
     * @exception IOException If an I/O error occurs while creating
     *                        the ServerSocket.
     ***/
    public ServerSocket createServerSocket(int port, int backlog,
                                           InetAddress bindAddr)
            throws IOException {
        return new ServerSocket(port, backlog, bindAddr);
    }
}
