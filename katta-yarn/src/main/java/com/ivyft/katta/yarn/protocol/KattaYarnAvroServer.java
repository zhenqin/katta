package com.ivyft.katta.yarn.protocol;

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
public class KattaYarnAvroServer extends SpecificResponder {




    protected String host = "0.0.0.0";


    protected int port = 7690;


    protected boolean daemon = false;



    protected final KattaYarnProtocol kattaYarnProtocol;



    private static Logger LOG = LoggerFactory.getLogger(KattaYarnAvroServer.class);


    public KattaYarnAvroServer(Class<? extends KattaYarnProtocol> iface, Object impl) {
        super(iface, impl);
        this.kattaYarnProtocol = (KattaYarnProtocol)impl;
    }


    public KattaYarnAvroServer(String iface, Object impl) throws ClassNotFoundException {
        this((Class<? extends KattaYarnProtocol>) Class.forName(iface), impl);
    }


    public void init() throws Exception {
        Runnable avroRun = new Runnable() {
            @Override
            public void run() {
                Server server = null;
                try {
                    server = new NettyServer(KattaYarnAvroServer.this, new InetSocketAddress(host, port));
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

    public KattaYarnProtocol getKattaYarnProtocol() {
        return kattaYarnProtocol;
    }

    public static void main(String[] args) throws Exception {
        KattaYarnAvroServer server = new KattaYarnAvroServer(KattaYarnProtocol.class,
                new KattaYarnMasterProtocol());
        server.setDaemon(true);
        server.init();
    }
}
