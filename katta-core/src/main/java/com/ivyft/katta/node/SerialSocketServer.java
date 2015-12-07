package com.ivyft.katta.node;

import com.ivyft.katta.lib.lucene.DefaultSocketPortFactory;
import com.ivyft.katta.lib.lucene.FreeSocketPortFactory;
import com.ivyft.katta.lib.lucene.LuceneServer;
import com.ivyft.katta.lib.lucene.SocketPortFactory;
import com.ivyft.katta.util.NodeConfiguration;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-10-16
 * Time: 下午3:09
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class SerialSocketServer extends Thread  {


    /**
     * Java Socket监听的端口
     */
    private int port = 5880;

    /**
     * Lucene Server
     */
    private final LuceneServer luceneServer;

    /**
     * 日志记录
     */
    private static Logger log = LoggerFactory.getLogger(SerialSocketServer.class);


    public SerialSocketServer(LuceneServer luceneServer, NodeConfiguration nodeConfiguration) {
        this.luceneServer = luceneServer;
        int port = nodeConfiguration.getInt(NodeConfiguration.EXPORT_SOCKET_PORT, 5880);
        SocketPortFactory factory = new FreeSocketPortFactory();
        int step = nodeConfiguration.getInt(NodeConfiguration.EXPORT_SOCKET_PORT + ".step", 1);

        this.port = factory.getSocketPort(port, step);
        nodeConfiguration.setProperty(NodeConfiguration.EXPORT_SOCKET_PORT, this.port);
    }

    @Override
    public void run() {
        ServerSocket serverSocket;

        try {
            serverSocket = new ServerSocket(port);
            log.info("socket listening port: " + port + "  server is starting.");
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }

        boolean run = true;
        while (run){
            try {
                Socket socket = serverSocket.accept();
                //传入Solr IndexSearcher
                luceneServer.submit(new SocketExportHandler(socket,
                        luceneServer.getShardBySolrPath(),
                        luceneServer.getSearcherHandlesByShard()));
            } catch (Exception e) {
                log.error(ExceptionUtils.getFullStackTrace(e));
            }
        }

        if(serverSocket != null) {
            try{
                serverSocket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public void setPort(int port) {
        this.port = port;
    }


    public int getPort() {
        return this.port;
    }
}
