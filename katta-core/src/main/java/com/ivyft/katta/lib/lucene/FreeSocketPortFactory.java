package com.ivyft.katta.lib.lucene;

import com.ivyft.katta.util.NetworkUtils;
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
public class FreeSocketPortFactory implements SocketPortFactory {



    private static Logger LOG = LoggerFactory.getLogger(FreeSocketPortFactory.class);

    public FreeSocketPortFactory() {
    }

    @Override
    public int getSocketPort(int port, int facetor) {
        int dstPort = port;


        while (true) {
            try {
                boolean portFree = NetworkUtils.isPortFree(dstPort);
                if(portFree) {
                    return dstPort;
                }

                LOG.warn("socket port: " + dstPort + " was used;");
                dstPort = dstPort + facetor;
            } catch (Exception e) {
                LOG.warn("socket exeception", e);
                dstPort = dstPort + facetor;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        //ServerSocket socket = new ServerSocket(5880);
        //ServerSocket socket2 = new ServerSocket(5890);

        int port = new FreeSocketPortFactory().getSocketPort(20000, 10);
        System.out.println(port);
        //ServerSocket socket3 = new ServerSocket(port);



    }

    
}
