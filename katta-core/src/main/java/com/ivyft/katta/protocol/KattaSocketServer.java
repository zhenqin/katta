package com.ivyft.katta.protocol;

import com.ivyft.katta.util.MasterConfiguration;
import com.ivyft.katta.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/1/6
 * Time: 18:24
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaSocketServer extends SpecificResponder {




    protected String host = "0.0.0.0";


    protected int port = 7690;


    protected boolean daemon = false;



    protected final KattaClientProtocol kattaClientProtocol;



    private static Logger LOG = LoggerFactory.getLogger(KattaSocketServer.class);


    public KattaSocketServer(Class<? extends KattaClientProtocol> iface, Object impl) {
        super(iface, impl);
        this.kattaClientProtocol = (KattaClientProtocol)impl;
    }


    public KattaSocketServer(String iface, Object impl) throws ClassNotFoundException {
        this((Class<? extends KattaClientProtocol>) Class.forName(iface), impl);
    }


    public void init() throws Exception {
        Runnable avroRun = new Runnable() {
            @Override
            public void run() {
                Server server = null;
                try {
                    server = new NettyServer(KattaSocketServer.this, new InetSocketAddress(host, port));
                    server.start();
                    LOG.info("start avro nio socket at: " + host + ":" + port);
                    server.join();
                } catch (InterruptedException e) {
                    if(server != null) {
                        server.close();
                    }
                }
            }
        };

        Thread avroThread = new Thread(avroRun);
        avroThread.setDaemon(true);
        avroThread.setName("katta-avro-server-thread");
        avroThread.start();

        if(daemon) {
            avroThread.join();
        }
    }


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }


    public boolean isDaemon() {
        return daemon;
    }

    public void setDaemon(boolean daemon) {
        this.daemon = daemon;
    }

    public KattaClientProtocol getKattaClientProtocol() {
        return kattaClientProtocol;
    }


    public static void main(String[] args) throws Exception {
        ZkConfiguration zkConf = new ZkConfiguration();

        ZkClient zkClient = new ZkClient(zkConf.getZKServers());


        InteractionProtocol protocol = new InteractionProtocol(zkClient, zkConf);
        KattaSocketServer server = new KattaSocketServer(KattaClientProtocol.class,
                new MasterStorageProtocol(new MasterConfiguration(), protocol));
        server.setDaemon(true);
        server.init();
    }
}
