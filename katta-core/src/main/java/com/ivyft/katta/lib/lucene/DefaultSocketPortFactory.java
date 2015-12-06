package com.ivyft.katta.lib.lucene;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/6
 * Time: 11:32
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class DefaultSocketPortFactory implements SocketPortFactory {



    private static Logger LOG = LoggerFactory.getLogger(DefaultSocketPortFactory.class);

    public DefaultSocketPortFactory() {
    }

    @Override
    public int getSocketPort(int port, int facetor) {
        int dstPort = port;


        ServerSocket socket = null;
        while (true) {
            try {
                socket = new ServerSocket(dstPort);
                break;
            } catch (BindException e) {
                LOG.warn("socket port: " + dstPort + " was used;");
                dstPort = dstPort + facetor;
                continue;
            } catch (IOException e) {
                LOG.error("", e);
            }
        }

        if(socket != null) {
            try {
                socket.close();
            } catch (IOException e) {

            }
            socket = null;
        }

        return dstPort;
    }

    public static void main(String[] args) throws Exception {
        ServerSocket socket = new ServerSocket(5880);
        ServerSocket socket2 = new ServerSocket(5890);

        int port = new DefaultSocketPortFactory().getSocketPort(5880, 10);
        System.out.println(port);
        ServerSocket socket3 = new ServerSocket(port);



    }

    
}
