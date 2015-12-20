package com.ivyft.katta.protocol;

import com.ivyft.katta.lib.writer.DataWriter;
import com.ivyft.katta.lib.writer.ShardRange;
import com.ivyft.katta.util.MasterConfiguration;
import com.ivyft.katta.util.ZkConfiguration;
import org.apache.avro.AvroRemoteException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/27
 * Time: 20:59
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class MasterStorageProtocol implements KattaClientProtocol, ConnectedComponent {

    protected final InteractionProtocol protocol;


    protected final MasterConfiguration conf;


    protected final Map<String, DataWriter> CACHED_INDEX_DATAWRITER_MAP = new ConcurrentHashMap<String, DataWriter>(3);



    protected final Map<String, CommitShards> COMMIT_TIMELINE_MAP = new ConcurrentHashMap<String, CommitShards>(3);



    protected final Class<? extends DataWriter> aClass;



    protected final Set<String> indices = new HashSet<String>(3);




    private static Logger LOG = LoggerFactory.getLogger(MasterStorageProtocol.class);


    public MasterStorageProtocol(MasterConfiguration conf, InteractionProtocol protocol) {
        this.conf = conf;
        this.protocol = protocol;

        this.aClass = (Class<? extends DataWriter>) conf.getClass("master.data.writer");

        LOG.info("master.data.writer: " + aClass.getName());

        indices.addAll(protocol.getIndices());
        indices.addAll(protocol.getNewIndexs());
        LOG.info("may blck index: " + indices.toString());

        reconnect();
    }



    @Override
    public void reconnect() {
        protocol.registerChildListener(this, ZkConfiguration.PathDef.NEW_INDICES, new IAddRemoveListener() {
            @Override
            public void added(String name) {
                indices.add(name);
            }

            @Override
            public void removed(String name) {
                indices.remove(name);
            }
        });


        protocol.registerChildListener(this, ZkConfiguration.PathDef.INDICES_METADATA, new IAddRemoveListener() {
            @Override
            public void added(String name) {
                indices.add(name);
            }

            @Override
            public void removed(String name) {
                indices.remove(name);
            }
        });
    }

    @Override
    public void disconnect() {
        protocol.unregisterComponent(this);
    }


    /**
     * 根据 Index Name 获取该 Index 的 Writer
     * @param index IndexName
     * @return 返回该 Index 的 Writer
     */
    private DataWriter getDataWriter(String index) {
        DataWriter dataWriter = CACHED_INDEX_DATAWRITER_MAP.get(index);
        if(dataWriter == null) {
            if(!indices.contains(index)) {
                throw new IllegalArgumentException("没有索引集: " + index);
            }
            try {
                dataWriter = aClass.newInstance();
                dataWriter.init(conf, protocol, index);
                CACHED_INDEX_DATAWRITER_MAP.put(index, dataWriter);
                LOG.info("new index writer instance, index name: " + index);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        //已经存在该 Writer, 这里应该检查是否可用, 并且不可用要等待可用
        //比如正在 commit 索引

        return dataWriter;
    }



    @Override
    public int add(Message message) throws AvroRemoteException {
        getDataWriter(message.getIndexId().toString()).write(message.getRowId().toString(), message.getPayload());
        return 1;
    }




    @Override
    public int addList(List<Message> messages) throws AvroRemoteException {
        int count = 0;
        for (Message message : messages) {
            count += this.add(message);
        }
        return count;
    }



    @Override
    public CharSequence comm(java.lang.CharSequence indexId) throws AvroRemoteException {
        try {
            DataWriter dataWriter = getDataWriter(indexId.toString());
            dataWriter.flush();
            String commitId = DateTime.now().toString("yyyyMMddHHmmss");
            Set<ShardRange> commits = dataWriter.commit(commitId);
            for (ShardRange commit : commits) {
                LOG.info("commit path: " + commit.getShardPath());
            }

            COMMIT_TIMELINE_MAP.put(commitId, new CommitShards(indexId.toString(), commitId, commits));
            return commitId;
        } catch (Exception e) {
            throw new AvroRemoteException(e);
        }
    }


    /** 提交成功, 启动创建索引进程 */
    @Override
    public java.lang.Void fsh(java.lang.CharSequence indexId, java.lang.CharSequence commitId) throws org.apache.avro.AvroRemoteException {
        try {
            CommitShards commitShards = COMMIT_TIMELINE_MAP.get(commitId.toString());
            if(commitShards != null) {
                //do something

                //可以提交创建索引了.
                LOG.info("finished index: "+ indexId + " commit: " + commitId);

                throw new IllegalStateException("unsupport finish option.");
            }

            COMMIT_TIMELINE_MAP.remove(commitId.toString());
        } catch (Exception e) {
            throw new AvroRemoteException(e);
        }

        return null;
    }

    @Override
    public Void roll(java.lang.CharSequence indexId, java.lang.CharSequence commitId) throws AvroRemoteException {
        try {
            DataWriter dataWriter = getDataWriter(indexId.toString());
            if(commitId == null) {
                dataWriter.rollback();
            } else {
                CommitShards commitShards = COMMIT_TIMELINE_MAP.get(commitId.toString());
                if(commitShards != null) {
                    dataWriter.rollback(commitShards.getCommits());
                }
                COMMIT_TIMELINE_MAP.remove(commitId.toString());
            }
        } catch (Exception e) {
            throw new AvroRemoteException(e);
        }
        return null;
    }

}
