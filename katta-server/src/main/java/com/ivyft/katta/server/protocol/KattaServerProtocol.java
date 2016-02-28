package com.ivyft.katta.server.protocol;

import com.ivyft.katta.lib.lucene.ILuceneServer;

import java.io.IOException;

/**
 *
 *
 * <p>
 *     该类区别{@link ILuceneServer}, 该类是子类保持 ILuceneServer 接口的工作, 同时需要一些增加管理 lucene
 *     索引的一些方法.
 * </p>
 *
 *
 *
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/2/26
 * Time: 15:49
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public interface KattaServerProtocol extends ILuceneServer {


    /**
     *
     * 添加一个索引集
     *
     * @param name 索引 Name(index#shard)
     * @param solrCollection solr core
     * @param uri hdfs path
     * @throws IOException
     */
    public void addShard(String name, String solrCollection, String uri) throws IOException;


    /**
     * 删除一个 shard, 会删除本地的 Lucene 索引, 因此在调用该方法前, 请确认是否要删除
     *
     * @param name shardName
     * @throws IOException
     */
    public void removeShard(String name) throws IOException;

}
