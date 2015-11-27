package com.ivyft.katta.client;


import com.ivyft.katta.protocol.KattaClientProtocol;
import com.ivyft.katta.protocol.Message;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
public class KattaClient implements KattaClientProtocol {


    private String host = "localhost";

    private int port = 7690;

    private final KattaClientProtocol kattaClientProtocol;

    private final Transceiver t;


    /**
     * LOG
     */
    private final static Log LOG = LogFactory.getLog(KattaClient.class);


    public KattaClient() {
        this("localhost", 7690);
    }


    public KattaClient(String host, int port) {
        this.host = host;
        this.port = port;

        try {
            this.t = new NettyTransceiver(new InetSocketAddress(host, port));
            this.kattaClientProtocol = SpecificRequestor.getClient(KattaClientProtocol.class,
                    new SpecificRequestor(KattaClientProtocol.class, t));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }


    @Override
    public int add(Message message) throws AvroRemoteException {
        return kattaClientProtocol.add(message);
    }

    @Override
    public int addList(List<Message> messages) throws AvroRemoteException {
        return kattaClientProtocol.addList(messages);
    }

    @Override
    public int commit() throws AvroRemoteException {
        return kattaClientProtocol.commit();
    }

    @Override
    public int rollback() throws AvroRemoteException {
        return kattaClientProtocol.rollback();
    }

    @Override
    public int close() throws AvroRemoteException {
        return kattaClientProtocol.close();
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
}
