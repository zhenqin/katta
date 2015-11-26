package com.ivyft.katta.client;

import com.ivyft.katta.lib.lucene.QueryResponse;
import com.ivyft.katta.util.KattaException;
import org.apache.solr.client.solrj.SolrQuery;

import java.util.Map;
import java.util.Set;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-25
 * Time: 下午4:14
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public interface ISolrClient extends ILuceneClient {


    /**
     * 查询Katta
     * @param query
     * @return
     */
    public QueryResponse query(SolrQuery query, String[] shards) throws KattaException;




    /**
     * 对一个字段做Group By操作。
     *
     * @param query   查询条件
     * @param indexNames 索引名称
     * @return 返回GroupBy的结果
     * @throws KattaException 可能引发的异常
     */
    public <E> Set<E> group(SolrQuery query, String[] indexNames) throws KattaException;



    /**
     * 对一个字段做Group By COUNT操作。
     *
     * @param query   查询条件
     * @param indexNames 索引名称
     * @return 返回GroupBy的结果
     * @throws KattaException 可能引发的异常
     */
    public <E> Map<E, Integer> facet(SolrQuery query, String[] indexNames) throws KattaException;




    /**
     * 对一个字段做Group By COUNT操作。
     *
     * @param query   查询条件
     * @param indexNames 索引名称
     * @return 返回GroupBy的结果
     * @throws KattaException 可能引发的异常
     */
    public <E> Map<E, Integer> facetRange(SolrQuery query, String[] indexNames) throws KattaException;



}
