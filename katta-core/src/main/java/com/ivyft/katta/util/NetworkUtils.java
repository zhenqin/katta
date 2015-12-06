package com.ivyft.katta.util;

import java.io.IOException;
import java.net.*;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/6
 * Time: 19:48
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public final class NetworkUtils {



    public final static String KATA_NODE_HOSTNAME = "katta.node.hostname.overwritten";

    public static boolean isPortFree(int port) {
        try {
            Socket socket = new Socket(NetworkUtils.getLocalhostName(), port);
            socket.close();
            return false;
        } catch (ConnectException e) {
            return true;
        } catch (SocketException e) {
            if (e.getMessage().equals("Connection reset by peer")) {
                return true;
            }
            throw new RuntimeException(e);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getLocalhostName() {
        String property = System.getProperty(KATA_NODE_HOSTNAME);
        if (property != null && property.trim().length() > 0) {
            return property;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            throw new RuntimeException("unable to retrieve localhost name");
        }
    }
}
