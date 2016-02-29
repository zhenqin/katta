package com.ivyft.katta.client;

import com.ivyft.katta.util.KattaException;

import java.net.URI;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/2/29
 * Time: 09:39
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public interface ISingleKattaClient {


    /**
     * 添加 Lucene 索引
     * @param name Lucene 索引
     * @param solr Solr Core
     * @param path HDFS Path
     */
    public void addIndex(String name, String solr, URI path) throws KattaException;


    /**
     * 删除索引, 删除索引会删除本地的 Lucene 索引.
     * @param name 索引名称, 一般为 'abc#edf'
     */
    public void removeIndex(String name) throws KattaException;
}
