/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ivyft.katta.client;

import com.ivyft.katta.lib.lucene.*;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.ClientConfiguration;
import com.ivyft.katta.util.KattaException;
import com.ivyft.katta.util.ZkConfiguration;
import org.apache.hadoop.io.MapWritable;
import org.apache.solr.client.solrj.SolrQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;

/**
 *
 * <p>
 *
 * Default implementation of {@link ILuceneClient}.
 *
 * </p>
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-13
 * Time: 上午8:58
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class LuceneClient implements ISolrClient {


    public static final int FIRST_ARG_SHARD_ARG_IDX = 0;


    public static final int SECOND_ARG_SHARD_ARG_IDX = 1;


    protected static final Method SEARCH_METHOD;

    static {
        try {
            SEARCH_METHOD = ILuceneServer.class.getMethod("search",
                    new Class[]{
                            QueryWritable.class,
                            String[].class,
                            Long.TYPE
                    });
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method search() in ILuceneSearch!");
        }
    }


    protected static final Method QUERY_METHOD;


    static {
        try {
            QUERY_METHOD = ILuceneServer.class.getMethod("query",
                    new Class[]{
                            QueryWritable.class,
                            String[].class,
                            Long.TYPE
                    });
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method query() in ILuceneSearch!");
        }
    }



    protected static final Method COUNT_METHOD;

    static {
        try {
            COUNT_METHOD = ILuceneServer.class.getMethod("count",
                    new Class[]{
                            QueryWritable.class,
                            String[].class,
                            Long.TYPE
                    });
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method count() in ILuceneSearch!");
        }
    }



    protected static final Method GROUP_METHOD;

    static {
        try {
            GROUP_METHOD = ILuceneServer.class.getMethod("group",
                    new Class[]{
                            QueryWritable.class,
                            String[].class,
                            Long.TYPE
                    });
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method count() in ILuceneSearch!");
        }
    }




    protected static final Method FACET_METHOD;
    protected static final Method FACET_METHOD_RANGE;

    static {
        try {
            FACET_METHOD = ILuceneServer.class.getMethod("facet",
                    new Class[]{
                            QueryWritable.class,
                            String[].class,
                            Long.TYPE
                    });
            FACET_METHOD_RANGE = ILuceneServer.class.getMethod("facetByRange",
                    new Class[]{
                            QueryWritable.class,
                            String[].class,
                            Long.TYPE
                    });
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method facet() or facetByRange() in ILuceneSearch!");
        }
    }

    /*
    * public MapWritable getDetails(String[] shards, int docId, String[] fields)
    * throws IOException; public MapWritable getDetails(String[] shards, int
    * docId) throws IOException;
    */
    protected static final Method GET_DETAILS_METHOD;
    protected static final Method GET_DETAILS_FIELDS_METHOD;

    static {
        try {
            GET_DETAILS_METHOD = ILuceneServer.class.getMethod("getDetail",
                    new Class[]{
                            String[].class,
                            Integer.TYPE
                    });

            GET_DETAILS_FIELDS_METHOD = ILuceneServer.class.getMethod("getDetail",
                    new Class[]{
                            String[].class,
                            Integer.TYPE,
                            String[].class});
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method getDetail() in ILuceneSearch!");
        }
    }

    /**
     * 搜索超时时间
     */
    protected long timeout = 5000;


    /**
     * 搜索的实例
     */
    protected Client client;


    /**
     * log
     */
    private final static Logger LOG = LoggerFactory.getLogger(LuceneClient.class);



    /**
     * default constructor
     */
    public LuceneClient() {
        this.client = new Client(getServerClass());
    }

    public LuceneClient(final INodeSelectionPolicy nodeSelectionPolicy) {
        this.client = new Client(getServerClass(), nodeSelectionPolicy);
    }

    public LuceneClient(InteractionProtocol protocol) {
        this.client = new Client(getServerClass(), protocol);
    }

    public LuceneClient(final ZkConfiguration zkConfig) {
        this.client = new Client(getServerClass(), zkConfig);
    }

    public LuceneClient(final INodeSelectionPolicy policy, final ZkConfiguration zkConfig) {
        this.client = new Client(getServerClass(), policy, zkConfig);
    }

    public LuceneClient(final INodeSelectionPolicy policy, final ZkConfiguration zkConfig,
                        ClientConfiguration clientConfiguration) {
        this.client = new Client(getServerClass(), policy, zkConfig, clientConfiguration);
    }



    /**
     * 查询Katta
     *
     * @param query
     * @return
     */
    @Override
    public QueryResponse query(SolrQuery query, String[] indexNames) throws KattaException {
        IResultReceiver<QueryResponse> results = this.client.broadcastToIndices(
                this.timeout,
                true,
                QUERY_METHOD,
                SECOND_ARG_SHARD_ARG_IDX,
                indexNames,
                new Object[]{
                        new QueryWritable(query),
                        null,
                        timeout
                });

        if (results.isError()) {
            throw results.getKattaException();
        }
        LOG.info(query.toString());
        return results.getResult();
    }

    @Override
    public Hits search(SolrQuery query, final String[] indexNames)
            throws KattaException {
        IResultReceiver<Hits> results;

        results = this.client.broadcastToIndices(
                this.timeout,
                true,
                SEARCH_METHOD,
                SECOND_ARG_SHARD_ARG_IDX,
                indexNames,
                new Object[]{
                        new QueryWritable(query),
                        null,
                        timeout
                });

        if (results.isError()) {
            throw results.getKattaException();
        }
        return results.getResult();
    }


    /**
     *
     * 按照query中的条件进行group。
     *
     * @param query
     * @param shards
     * @return
     */
    public FacetResultWritable group(QueryWritable query, String[] shards) {
        throw new UnsupportedOperationException("该方法目前还不被支持。");
    }



    @Override
    public int count(final SolrQuery query, final String[] indexNames) throws KattaException {
        IResultReceiver<Integer> results = this.client.broadcastToIndices(
                this.timeout,
                true,
                COUNT_METHOD,
                SECOND_ARG_SHARD_ARG_IDX,
                indexNames,
                new Object[]{
                        new QueryWritable(query),
                        null,
                        timeout
                });
        if (results.isError()) {
            throw results.getKattaException();
        }
        return results.getResult();
    }


    @Override
    public MapWritable getDetail(final Hit hit) throws KattaException {
        return getDetail(hit, null);
    }

    @Override
    public MapWritable getDetail(final Hit hit, final String[] fields) throws KattaException {
        int docId = hit.getDocId();
        //
        Object[] args;
        Method method;
        int shardArgIdx;
        if (fields == null) {
            args = new Object[]{null, Integer.valueOf(docId)};
            method = GET_DETAILS_METHOD;
            shardArgIdx = FIRST_ARG_SHARD_ARG_IDX;
        } else {
            args = new Object[]{null, Integer.valueOf(docId), fields};
            method = GET_DETAILS_FIELDS_METHOD;
            shardArgIdx = FIRST_ARG_SHARD_ARG_IDX;
        }
        IResultReceiver<MapWritable> results = this.client.broadcastToShards(
                this.timeout,
                true,
                method,
                shardArgIdx,
                Arrays.asList(hit.getShard()),
                args);
        if (results.isError()) {
            throw results.getKattaException();
        }
        return results.getResult();
    }

    @Override
    public List<MapWritable> getDetails(List<Hit> hits) throws KattaException, InterruptedException {
        return getDetails(hits, null);
    }

    @Override
    public List<MapWritable> getDetails(List<Hit> hits, final String[] fields) throws KattaException,
            InterruptedException {
        List<MapWritable> results = new ArrayList<MapWritable>();
        for (final Hit hit : hits) {
            results.add(getDetail(hit, fields));
        }
        return results;
    }


    /**
     * 对一个字段做Group By操作。
     *
     * @param query   查询条件
     * @param indexNames 索引名称
     * @return 返回GroupBy的结果
     * @throws KattaException 可能引发的异常
     */
    @Override
    public <E> Set<E> group(final SolrQuery query, final String[] indexNames) throws KattaException {
        IResultReceiver<Set<E>> results = this.client.broadcastToIndices(
                this.timeout,
                true,
                GROUP_METHOD,
                SECOND_ARG_SHARD_ARG_IDX,
                indexNames,
                new Object[]{
                        new QueryWritable(query),
                        null,
                        timeout
                });
        if (results.isError()) {
            throw results.getKattaException();
        }
        return results.getResult();
    }



    /**
     * 对一个字段做Group By COUNT操作。
     *
     * @param query   查询条件
     * @param indexNames 索引名称
     * @return 返回GroupBy的结果
     * @throws KattaException 可能引发的异常
     */
    @Override
    public <E> Map<E, Integer> facet(final SolrQuery query, final String[] indexNames) throws KattaException {
        IResultReceiver<Map<E, Integer>> results = this.client.broadcastToIndices(
                this.timeout,
                true,
                FACET_METHOD,
                SECOND_ARG_SHARD_ARG_IDX,
                indexNames,
                new Object[]{
                        new QueryWritable(query),
                        null,
                        timeout
                });
        if (results.isError()) {
            throw results.getKattaException();
        }
        return results.getResult();
    }



    /**
     * 对一个字段做Group By COUNT操作。
     *
     * @param query   查询条件
     * @param indexNames 索引名称
     * @return 返回GroupBy的结果
     * @throws KattaException 可能引发的异常
     */
    @Override
    public <E> Map<E, Integer> facetRange(SolrQuery query, String[] indexNames) throws KattaException {
        IResultReceiver<Map<E, Integer>> results = this.client.broadcastToIndices(
                this.timeout,
                true,
                FACET_METHOD_RANGE,
                SECOND_ARG_SHARD_ARG_IDX,
                indexNames,
                new Object[]{
                        new QueryWritable(query),
                        null,
                        timeout
                });
        if (results.isError()) {
            throw results.getKattaException();
        }
        return results.getResult();
    }




    @Override
    public double getQueryPerMinute() {
        return this.client.getQueryPerMinute();
    }


    @Override
    public void close() {
        this.client.close();
    }

    public Client getClient() {
        return this.client;
    }


    public <T> KattaLoader<T> getKattaLoader(String index) {
        return client.getKattaLoader(index);
    }

    public InteractionProtocol getProtocol() {
        return getClient().getProtocol();
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }


    protected Class<? extends ILuceneServer> getServerClass() {
        return ILuceneServer.class;
    }


    private static Method getMethod(String name, Class<?>... parameterTypes) {
        try {
            return ILuceneServer.class.getMethod(name, parameterTypes);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method " + name + "(" + Arrays.asList(parameterTypes)
                    + ") in ILuceneSearch!");
        }
    }

}
