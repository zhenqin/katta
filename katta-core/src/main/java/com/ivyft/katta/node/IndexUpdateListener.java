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



    public void onBeforeUpdate(String indexName, String shardName);




    public void onAfterUpdate(String indexName, String shardName);
}
