package com.ivyft.katta.lib.writer;

import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.HadoopUtil;
import com.ivyft.katta.util.MasterConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/29
 * Time: 11:37
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public abstract class DataWriter {



    private static Logger LOG = LoggerFactory.getLogger(DataWriter.class);


    /**
     * Wirter 初始化
     * @param conf Katta Master 配置
     * @param protocol Katta Protocol 协议
     * @param indexName Index Name
     */
    public abstract void init(MasterConfiguration conf, InteractionProtocol protocol, String indexName);


    /**
     * 向该 Writer 写数据
     * @param shardId shardId, 即数据分隔 ID
     * @param objByte 数据内容
     */
    public abstract void write(String shardId, ByteBuffer objByte);



    /**
     * flush Data Writer
     */
    public abstract void flush();



    /**
     * Commit 会转移所有数据
     */
    public synchronized Set<ShardRange> commit(String commitId) {
        return reset("commit-", commitId);
    }



    /**
     * rollback 会丢弃目前所有 Commit 写入的数据.
     *
     * @throws IllegalStateException
     */
    public synchronized void rollback() {
        String commitId = DateTime.now().toString("yyyyMMddHHmmss");
        Set<ShardRange> shardRanges = reset("rollback-", commitId);
        try {
            FileSystem fs = HadoopUtil.getFileSystem();
            for (ShardRange shardRange : shardRanges) {
                LOG.warn("delete rollback shard: " + shardRange.getShardName() + " path: " + shardRange.getShardPath());
                fs.delete(new Path(shardRange.getShardPath()), true);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }



    /**
     * rollback 会丢弃目前所有 Commit 写入的数据.
     *
     * @throws IllegalStateException
     */
    public synchronized void rollback(Set<ShardRange> shardRanges) {
        try {
            FileSystem fs = HadoopUtil.getFileSystem();
            for (ShardRange shardRange : shardRanges) {
                LOG.warn("delete rollback shard: " + shardRange.getShardName() + " path: " + shardRange.getShardPath());
                fs.delete(new Path(shardRange.getShardPath()), true);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }



    /**
     * commit or rockback 调用
     * @param actionName commit or rollback
     * @return 返回当前正在操作的 ShardRange
     */
    protected abstract Set<ShardRange> reset(String actionName, String commitId);



    /**
     * Close Data Writer
     */
    public abstract void close();
}
