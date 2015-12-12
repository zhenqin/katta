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
    public Void stopAllMaster() throws AvroRemoteException {
        return this.kattaYarnProtocol.stopAllMaster();
    }


    @Override
    public List<KattaAndNode> listMasters() throws AvroRemoteException {
        return this.kattaYarnProtocol.listMasters();
    }


    @Override
    public List<KattaAndNode> listNodes() throws AvroRemoteException {
        return this.kattaYarnProtocol.listNodes();
    }

    @Override
    public Void stopMaster(java.lang.CharSequence id, com.ivyft.katta.yarn.protocol.IdType idType) throws AvroRemoteException {
        return this.kattaYarnProtocol.stopMaster(id, idType);
    }

    @Override
    public Void stopNode(java.lang.CharSequence id, com.ivyft.katta.yarn.protocol.IdType idType) throws AvroRemoteException {
        return this.kattaYarnProtocol.stopNode(id, idType);
    }

    @Override
    public Void addNode(int memory, int cores, java.lang.CharSequence kattaZip, java.lang.CharSequence solrZip) throws AvroRemoteException {
        return this.kattaYarnProtocol.addNode(memory, cores, kattaZip, solrZip);
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
