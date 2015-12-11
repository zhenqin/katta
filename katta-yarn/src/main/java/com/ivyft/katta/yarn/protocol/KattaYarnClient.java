package com.ivyft.katta.yarn.protocol;


import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;


/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/27
 * Time: 19:26
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaYarnClient implements KattaYarnProtocol {


    private String host = "localhost";

    private int port = 7690;


    private final KattaYarnProtocol kattaYarnProtocol;


    private final Transceiver t;


    /**
     * LOG
     */
    private final static Logger LOG = LoggerFactory.getLogger(KattaYarnClient.class);


    public KattaYarnClient() {
        this("localhost", 7690);
    }


    public KattaYarnClient(String host, int port) {
        this.host = host;
        this.port = port;
        try {
            this.t = new NettyTransceiver(new InetSocketAddress(host, port));
            this.kattaYarnProtocol = SpecificRequestor.getClient(KattaYarnProtocol.class,
                    new SpecificRequestor(KattaYarnProtocol.class, t));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }


    @Override
    public Void startMaster(int memory, int cores, java.lang.CharSequence kattaZip) throws AvroRemoteException {
        return this.kattaYarnProtocol.startMaster(memory, cores, kattaZip);
    }

    @Override
    public Void registerMaster(KattaAndNode kattaNode) throws AvroRemoteException {
        return null;
    }

    @Override
    public List<KattaAndNode> listMasters() throws AvroRemoteException {
        return null;
    }

    @Override
    public Void unregisterMaster(KattaAndNode kattaNode) throws AvroRemoteException {
        return null;
    }

    @Override
    public Void registerNode(KattaAndNode kattaNode) throws AvroRemoteException {
        return null;
    }

    @Override
    public Void unregisterNode(KattaAndNode kattaNode) throws AvroRemoteException {
        return null;
    }

    @Override
    public List<KattaAndNode> listNodes() throws AvroRemoteException {
        return null;
    }

    @Override
    public Void stopMaster() throws AvroRemoteException {
        return this.kattaYarnProtocol.stopMaster();
    }

    @Override
    public Void stopNode() throws AvroRemoteException {
        return this.kattaYarnProtocol.stopNode();
    }

    @Override
    public Void addNode(int memory, int cores, java.lang.CharSequence kattaZip) throws AvroRemoteException {
        return this.kattaYarnProtocol.addNode(memory, cores, kattaZip);
    }

    @Override
    public Void stopAllNode() throws AvroRemoteException {
        return this.kattaYarnProtocol.stopAllNode();
    }

    @Override
    public Void shutdown() throws AvroRemoteException {
        return this.kattaYarnProtocol.shutdown();
    }


    @Override
    public Void close() throws AvroRemoteException {
        try {
            this.t.close();
            return null;
        } catch (IOException e) {
            throw  new AvroRemoteException(e);
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


    public static void main(String[] args) throws AvroRemoteException {
        KattaYarnClient client = new KattaYarnClient("localhost", 4880);
        client.shutdown();
        client.close();
    }

}
