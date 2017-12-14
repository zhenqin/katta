package com.ivyft.katta.client;


import com.ivyft.katta.codec.Serializer;
import com.ivyft.katta.lib.writer.Serialization;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.KattaClientProtocol;
import com.ivyft.katta.protocol.Message;
import com.ivyft.katta.util.ClientConfiguration;
import com.ivyft.katta.util.ZkConfiguration;
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
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;


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
public class KattaClient<T> extends LuceneClient implements KattaClientProtocol, KattaLoader<T> {


    /**
     * Master  的 IP 地址
     */
    private String host = "localhost";


    /**
     * Master 打开的端口号
     */
    private int port = 8440;


    /**
     * 和远程 Master 的通信协议
     */
    private KattaClientProtocol kattaClientProtocol;


    /**
     * 和远程 Master 的内部通信
     */
    private Transceiver t;


    /**
     * 数据序列化方法
     */
    protected Serializer serializer;


    /**
     * 当前 DataLoader 打开的索引
     */
    private String indexName;


    /**
     * 当前的 DataLoader 的 commit 提交
     */
    private CharSequence currentCommitId;


    /**
     * 是否当前打开的只是 DataLoader
     */
    private boolean onlyLoader;

    /**
     * LOG
     */
    private final static Logger LOG = LoggerFactory.getLogger(KattaClient.class);


    public KattaClient(INodeSelectionPolicy nodeSelectionPolicy) {
        super(nodeSelectionPolicy);
    }

    public KattaClient(InteractionProtocol protocol) {
        super(protocol);
    }

    public KattaClient(ZkConfiguration zkConfig) {
        super(zkConfig);
    }

    public KattaClient(INodeSelectionPolicy policy, ZkConfiguration zkConfig) {
        super(policy, zkConfig);
    }

    public KattaClient(INodeSelectionPolicy policy, ZkConfiguration zkConfig, ClientConfiguration clientConfiguration) {
        super(policy, zkConfig, clientConfiguration);
    }

    protected KattaClient(String host, int port, String index) {
        this.host = host;
        this.port = port;
        this.onlyLoader = true;
        this.indexName = index;
        if(StringUtils.isBlank(index)) {
            throw new IllegalArgumentException("index must not be blank.");
        }

        Iterator iterator = ServiceLoader.load(Serialization.class).iterator();
        Serialization serialization = (Serialization)iterator.next();

        LOG.info("Serialization class: " + serialization.getClass().getName());
        try {
            serializer = serialization.serialize();
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
    public int addBean(String shardId, Object message) {
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
    public void finish(int timeout, TimeUnit unit) {
        if(currentCommitId == null) {
            throw new IllegalArgumentException("currentCommitId is null, please commit first.");
        }
        try {
             kattaClientProtocol.fsh(indexName, currentCommitId, unit.toMillis(timeout));
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
    public Void fsh(CharSequence indexId, CharSequence commitId, long timeout) throws AvroRemoteException {
        return kattaClientProtocol.fsh(indexId, commitId, timeout);
    }

    @Override
    public Void roll(java.lang.CharSequence indexId, java.lang.CharSequence commitId) throws AvroRemoteException {
        return kattaClientProtocol.roll(indexId, commitId);
    }


    @Override
    public void close() throws IOException {
        if(this.onlyLoader && t != null) {
            t.close();
            t = null;
            kattaClientProtocol = null;
            return;
        }

        super.close();
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
