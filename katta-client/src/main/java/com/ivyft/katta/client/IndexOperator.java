package com.ivyft.katta.client;

import com.ivyft.katta.lib.lucene.*;
import com.ivyft.katta.util.KattaException;
import com.ivyft.katta.util.ZkConfiguration;
import org.apache.hadoop.io.MapWritable;
import org.apache.solr.client.solrj.SolrQuery;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 17/12/17
 * Time: 09:49
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class IndexOperator {


    /**
     * 远程连接
     */
    protected final KattaClient kattaClient;


    /**
     * 操作的索引分片
     */
    protected final String[] indices;


    public IndexOperator(ZkConfiguration zkConfig, String index) {
        this(new KattaClient(zkConfig), index);
    }

    public IndexOperator(INodeSelectionPolicy policy, ZkConfiguration zkConfig, String index) {
        this(new KattaClient(policy, zkConfig), index);
    }

    public IndexOperator(KattaClient kattaClient, String index) {
        this(kattaClient, new String[]{index});
    }

    public IndexOperator(KattaClient kattaClient, String[] indices) {
        this.kattaClient = kattaClient;
        this.indices = indices;
    }

    public QueryResponse query(SolrQuery query) throws KattaException {
        return kattaClient.query(query, indices);
    }

    public Hits search(SolrQuery query) throws KattaException {
        return kattaClient.search(query, indices);
    }

    public FacetResultWritable group(QueryWritable query, String[] shards) {
        return kattaClient.group(query, shards);
    }

    public int count(SolrQuery query) throws KattaException {
        return kattaClient.count(query, indices);
    }

    public MapWritable getDetail(Hit hit) throws KattaException {
        return kattaClient.getDetail(hit);
    }

    public MapWritable getDetail(Hit hit, String[] fields) throws KattaException {
        return kattaClient.getDetail(hit, fields);
    }

    public List<MapWritable> getDetails(List<Hit> hits) throws KattaException, InterruptedException {
        return kattaClient.getDetails(hits);
    }

    public List<MapWritable> getDetails(List<Hit> hits, String[] fields) throws KattaException, InterruptedException {
        return kattaClient.getDetails(hits, fields);
    }

    public <E> Set<E> group(SolrQuery query) throws KattaException {
        return kattaClient.group(query, indices);
    }

    public <E> Map<E, Integer> facet(SolrQuery query) throws KattaException {
        return kattaClient.facet(query, indices);
    }

    public <E> Map<E, Integer> facetRange(SolrQuery query) throws KattaException {
        return kattaClient.facetRange(query, indices);
    }

    public void close() throws IOException {
        kattaClient.close();
    }

    public KattaClient getKattaClient() {
        return kattaClient;
    }
}
