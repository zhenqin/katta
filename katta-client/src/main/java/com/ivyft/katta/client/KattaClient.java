package com.ivyft.katta.client;


import com.ivyft.katta.codec.Serializer;
import com.ivyft.katta.codec.jdkserializer.JdkSerializer;
import com.ivyft.katta.protocol.KattaClientProtocol;
import com.ivyft.katta.protocol.Message;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
public class KattaClient<T> implements KattaClientProtocol, KattaLoader<T> {


    private String host = "localhost";

    private int port = 8440;


    private final KattaClientProtocol kattaClientProtocol;


    private final Transceiver t;


    protected Serializer serializer = new JdkSerializer();


    private final String indexName;



    private CharSequence currentCommitId;

    /**
     * LOG
     */
    private final static Logger LOG = LoggerFactory.getLogger(KattaClient.class);


    public KattaClient(String indexName) {
        this("localhost", 8440, indexName);
    }


    public KattaClient(String host, int port, String index) {
        this.host = host;
        this.port = port;
        this.indexName = index;
        if(StringUtils.isBlank(index)) {
            throw new IllegalArgumentException("index must not be blank.");
        }
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
    public int add(Pair<T> pair) {
        return addBean(pair.getShardId(), pair.getBean());
    }

    @Override
    public int addBean(String shardId, T message) {
        try {
            return add(new Message(this.indexName, shardId, ByteBuffer.wrap(serializer.serialize(message))));
        } catch (AvroRemoteException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public int addBeans(List<Pair<T>> list) {
        List<Message> messages = new ArrayList<Message>(list.size());
        for (Pair d : list) {
            messages.add(new Message(this.indexName, d.getShardId(),
                    ByteBuffer.wrap(serializer.serialize(d.getBean()))));
        }
        try {
            return addList(messages);
        } catch (AvroRemoteException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void commit() {
        try {
            currentCommitId = kattaClientProtocol.comm(indexName);
        } catch (AvroRemoteException e) {
            throw new IllegalStateException(e);
        }
    }



    @Override
    public void finish() {
        if(currentCommitId == null) {
            throw new IllegalArgumentException("currentCommitId is null, please commit first.");
        }
        try {
             kattaClientProtocol.fsh(indexName, currentCommitId);
            currentCommitId = null;
        } catch (AvroRemoteException e) {
            throw new IllegalStateException(e);
        }
    }



    @Override
    public void rollback() {
        if(currentCommitId == null) {
            LOG.warn("currentCommitId is null, rollback all of blck data.");
        }
        try {
            kattaClientProtocol.roll(indexName, currentCommitId);
            currentCommitId = null;
        } catch (AvroRemoteException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public CharSequence comm(java.lang.CharSequence indexId) throws AvroRemoteException {
        return kattaClientProtocol.comm(indexId);
    }

    @Override
    public Void fsh(CharSequence indexId, CharSequence commitId) throws AvroRemoteException {
        return kattaClientProtocol.fsh(indexId, commitId);
    }

    @Override
    public Void roll(java.lang.CharSequence indexId, java.lang.CharSequence commitId) throws AvroRemoteException {
        return kattaClientProtocol.roll(indexId, commitId);
    }


    public void close() throws IOException {
        t.close();
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
