package com.ivyft.katta.node;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/4/26
 * Time: 12:21
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public interface IndexUpdateListener {


    /**
     * 当索引被修改前调用
     * @param indexName 被修改的 Index
     * @param shardName 被修改的 Index Shard
     */
    public void onBeforeUpdate(String indexName, String shardName);


    /**
     * 索引修改完成后调用
     * @param indexName 被修改的 Index
     * @param shardName 被修改的 Index Shard
     */
    public void onAfterUpdate(String indexName, String shardName);
}
