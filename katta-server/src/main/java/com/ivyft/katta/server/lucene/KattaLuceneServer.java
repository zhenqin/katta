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
package com.ivyft.katta.server.lucene;

import com.ivyft.katta.lib.lucene.*;
import com.ivyft.katta.lib.lucene.convertor.DocumentConvertor;
import com.ivyft.katta.node.IContentServer;
import com.ivyft.katta.node.IndexUpdateListener;
import com.ivyft.katta.node.ShardManager;
import com.ivyft.katta.server.protocol.KattaServerProtocol;
import com.ivyft.katta.util.NodeConfiguration;
import com.ivyft.katta.util.ReflectionUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.MapWritable;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.client.solrj.SolrQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 *
 * The back end server which searches a set of Lucene indices. Each shard is a
 * Lucene index directory.
 * </p>
 *
 *
 * <p>
 *
 * Normal usage is to first call getDocFreqs() to get the global term
 * frequencies, then pass that back in to search(). This way you get uniform
 * scoring across all the nodes / instances of KattaLuceneServer.
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
 * @author ZhenQin
 *
 */
public class KattaLuceneServer extends LuceneServer implements IContentServer, KattaServerProtocol, IndexUpdateListener {

    /**
     * Copy 索引
     */
    protected ShardManager shardManager;


    /**
     * Log
     */
    private final static Logger LOG = LoggerFactory.getLogger(KattaLuceneServer.class);


    /**
     * 默认构造方法必须有，用反射实例化
     *
     */
    public KattaLuceneServer() {
        super();
    }

    /**
     *
     * Constructor for testing purpose, {@link #init(String, NodeConfiguration)}
     * need not to be called.
     *
     * @param name Index Name
     * @param seacherFactory Lucene Search Factory
     * @param timeoutPercentage Tile
     */
    public KattaLuceneServer(String name, ISeacherFactory seacherFactory, float timeoutPercentage) {
        super(name, seacherFactory, timeoutPercentage);
        init(name, new NodeConfiguration());
    }



    @Override
    public void init(String nodeName, NodeConfiguration nodeConfiguration) {
        if(threadPool == null) {
            super.init(nodeName, nodeConfiguration);
        }
        if(shardManager == null) {
            throw new NullPointerException("shardManager is null");
        }
    }

    @Override
    public void onBeforeUpdate(String indexName, String shardName) {

    }

    @Override
    public void onAfterUpdate(String indexName, String shardName) {

    }



    @Override
    public void addShard(String name, String solrCollection, String uri, boolean  merge) throws IOException {
        LOG.info("index name {}, solr core {}, path {}", name, solrCollection, uri);
        try {
            URI localUri = shardManager.installShard2(name, uri, merge);
            LOG.info("install shard {} to {}", name, localUri.toString());

            addShard(name, localUri, solrCollection);

            File localIndexDir = new File(localUri);
            File shardMeta = new File(localIndexDir, ".shard.info");
            FileUtils.write(shardMeta, name + "\n" + solrCollection + "\n" + uri + "\n\n", "UTF-8", merge);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }




    /**
     * Returns the <code>IndexHandle</code> of the given shardName.
     *
     * @param shardName the name of the shard
     * @return the <code>IndexHandle</code> of the given shardName
     */
    @Override
    public SearcherHandle getSearcherHandleByShard(String shardName) {
        SearcherHandle handle = searcherHandlesByShard.get(shardName);
        if(handle == null) {
            //TODO 有可能本地有 Lucene 索引, 如果有, 则把索引加入到searcherHandlesByShard
            File shardsFolder = shardManager.getShardsFolder();
            File shardIndexDir = new File(shardsFolder, shardName);
            if(shardIndexDir.exists() && shardIndexDir.isDirectory()) {
                LOG.info("exists local index dir {}", shardIndexDir.getAbsolutePath());
                File shardInfo = new File(shardIndexDir, ".shard.info");
                try {
                    List<String> lines = FileUtils.readLines(shardInfo, "UTF-8");
                    LOG.info(StringUtils.join(lines, "; "));
                    if(lines.size() >= 3) {
                        //这里相当于本地索引安装，本地索引当在磁盘上存在时，只会按照本地模式安装
                        addShard(StringUtils.trimToEmpty(lines.get(0)),
                                 StringUtils.trimToEmpty(lines.get(1)),
                                StringUtils.trimToEmpty(shardIndexDir.toURI().toString()), false);

                        handle = searcherHandlesByShard.get(shardName);
                    }
                } catch (IOException e) {
                    LOG.warn("", e);
                }
            }
        }

        if (handle == null) {
            throw new IllegalStateException("no index-server for shard '" +
                    shardName + "' found - probably undeployed");
        }
        return handle;
    }


    /**
     * Returns the <code>IndexHandle</code> of the given shardName.
     *
     * @param shardName the name of the shard
     * @return the <code>IndexHandle</code> of the given shardName
     */
    @Override
    public SolrHandler getSolrHandlerByShard(String shardName) {
        SolrHandler handle = shardBySolrPath.get(shardName);
        if(handle == null) {
            File shardsFolder = shardManager.getShardsFolder();
            File shardIndexDir = new File(shardsFolder, shardName);
            if(shardIndexDir.exists() && shardIndexDir.isDirectory()) {
                File shardInfo = new File(shardIndexDir, ".shard.info");
                try {
                    List<String> lines = FileUtils.readLines(shardInfo, "UTF-8");
                    if(lines.size() >= 3) {
                        //这里相当于本地索引安装，本地索引当在磁盘上存在时，只会按照本地模式安装
                        addShard(StringUtils.trimToEmpty(lines.get(0)),
                                StringUtils.trimToEmpty(lines.get(1)),
                                StringUtils.trimToEmpty(shardIndexDir.toURI().toString()), false);

                        handle = shardBySolrPath.get(shardName);
                    }
                } catch (IOException e) {
                    LOG.warn("", e);
                }
            }
        }

        if (handle == null) {
            throw new IllegalStateException("no index-server for shard '" +
                    shardName + "' found - probably undeployed");
        }
        return handle;
    }


    public ShardManager getShardManager() {
        return shardManager;
    }

    public void setShardManager(ShardManager shardManager) {
        this.shardManager = shardManager;
    }



    @Override
    public HitsMapWritable search(QueryWritable query,
                                  String[] shardNames,
                                  long timeout)
            throws IOException {
        getSearcherHandleByShard(shardNames[0]);
        int count = query.getQuery().getRows() == null ? LuceneServer.DEFAULT_QUERY.getRows() : query.getQuery().getRows();
        return super.search(query, shardNames, timeout, count);
    }

    @Override
    public HitsMapWritable search(QueryWritable query,
                                  String[] shards,
                                  long timeout,
                                  int count) throws IOException {
        getSearcherHandleByShard(shards[0]);
        return super.search(query, shards, timeout, count);
    }

    /**
     * 获取全部的字段信息
     * @param shards The shards to ask for the document.
     * @param docId  The document that is desired.
     * @return
     * @throws IOException
     */
    @Override
    public MapWritable getDetail(String[] shards,
                                 int docId) throws IOException {
        getSearcherHandleByShard(shards[0]);
        return super.getDetail(shards, docId, null);
    }


        /**
         * 获取指定的字段信息
         * @param shards The shards to ask for the document.
         * @param docId  The document that is desired.
         * @param fieldNames
         * @return
         * @throws IOException
         */
    @Override
    public MapWritable getDetail(String[] shards,
                                  int docId,
                                  String[] fieldNames) throws IOException {
        getSearcherHandleByShard(shards[0]);
        return super.getDetail(shards, docId, fieldNames);
    }



    @Override
    public int count(QueryWritable query,
                     String[] shards,
                     long timeout) throws IOException {
        getSearcherHandleByShard(shards[0]);
        return search(query, shards, timeout, 1).getTotalHits();
    }


    /**
     * Search in the given shards and return max hits for given query
     *
     * @param query
     * @param shards
     * @param result
     * @param limit
     * @throws IOException
     */
    @Override
    protected final void search(SolrQuery query,
                                String[] shards,
                                HitsMapWritable result,
                                int limit,
                                long timeout) throws IOException {
        getSearcherHandleByShard(shards[0]);
        int offset = query.getStart() == null ? 0 : query.getStart();

        timeout = getCollectorTiemout(timeout);
        Query luceneQuery;
        try {
            luceneQuery = parse(query, shards[0]);
        } catch (Exception e) {
            throw new IOException(e);
        }

        int shardsCount = shards.length;


        // Run the search in parallel on the shards with a thread pool.
        CompletionService<SearchResult> csSearch = new ExecutorCompletionService<SearchResult>(threadPool);

        //这里是并行搜索
        for (int i = 0; i < shardsCount; i++) {
            SearchCall call = new SearchCall(shards[i], offset, limit, timeout, i, luceneQuery, null);
            csSearch.submit(call);
        }

        //final ScoreDoc[][] scoreDocs = new ScoreDoc[shardsCount][];
        ScoreDoc scoreDocExample = null;

        //查询到的总数, numFount
        AtomicInteger totalHits = new AtomicInteger(0);
        for (int i = 0; i < shardsCount; i++) {
            try {
                SearchResult searchResult = csSearch.take().get();
                int callIndex = searchResult.getSearchCallIndex();

                //获取每一个shard的文档总数
                totalHits.addAndGet(searchResult.getTotalHits());

                //搜索的结果, 都在这里保存着
                ScoreDoc[] scoreDocs = searchResult.getScoreDocs();
                if (scoreDocExample == null && scoreDocs.length > 0) {
                    scoreDocExample = scoreDocs[0];
                }
                for (ScoreDoc doc : scoreDocs) {
                    result.addHit(new Hit(shards[callIndex], getNodeName(), doc.score, doc.doc));
                }
            } catch (InterruptedException e) {
                throw new IOException("Multithread shard search interrupted:", e);
            } catch (ExecutionException e) {
                throw new IOException("Multithread shard search could not be executed:", e);
            }
        }
        //发现总数
        result.addTotalHits(totalHits.get());
    }


    @Override
    public ResponseWritable query(QueryWritable query,
                                  String[] shards,
                                  long timeout) throws IOException {
        getSearcherHandleByShard(shards[0]);
        SolrQuery solrQuery = query.getQuery();
        LOG.info("Lucene query: " + solrQuery.toString() + " shards: [" + StringUtils.join(shards, ",") +"]");

        long start = System.currentTimeMillis();

        QueryResponse response = new QueryResponse();

        //启动多线程搜索
        query(solrQuery, shards, response, timeout);

        //计时
        response.setQTime(System.currentTimeMillis() - start);
        LOG.debug("Complete search took " + response.getQTime() + "ms.");
        return new ResponseWritable(response);
    }
    /**
     * Search in the given shards and return max hits for given query
     *
     * @param query
     * @param shards
     * @param response
     * @throws IOException
     */
    @Override
    protected final void query(SolrQuery query,
                                String[] shards,
                                QueryResponse response,
                                long timeout) throws IOException {
        getSearcherHandleByShard(shards[0]);
        timeout = getCollectorTiemout(timeout);
        Query luceneQuery;

        //处理搜索时，包含的返回字段
        String fieldString = query.getFields();
        Set<String> fields = null;
        if(StringUtils.isNotBlank(fieldString)) {
            fields = new HashSet<String>(Arrays.asList(fieldString.split(",")));
        }

        int offset = query.getStart() == null ? 0 : query.getStart();
        int limit = query.getRows() == null ? LuceneServer.DEFAULT_QUERY.getRows() : query.getRows();


        //解析 SolrQuery 为 Lucene 的 Query 对象
        try {
            luceneQuery = parse(query, shards[0]);
        } catch (Exception e) {
            throw new IOException(e);
        }


        //排序策略
        List<SolrQuery.SortClause> sorts = query.getSorts();
        Sort sort = null;
        if(sorts.size() > 0) {
            SolrHandler handler = shardBySolrPath.get(shards[0]);


            sort = new Sort();
            SortField[] sortFields = new SortField[sorts.size()];
            int m = 0;
            for (SolrQuery.SortClause sortClause : sorts) {
                TypeSource typeSource = handler.getFieldGroup(sortClause.getItem());

                SortField.Type fieldType = typeSource.getFieldType();

                if(fieldType == SortField.Type.DOC) {
                    sortFields[m] = new SortField(sortClause.getItem(),
                            new TextFieldComparatorSource(),
                            sortClause.getOrder() == SolrQuery.ORDER.desc ? true : false);
                } else {
                    sortFields[m] = new SortField(sortClause.getItem(),
                            fieldType,
                            sortClause.getOrder() == SolrQuery.ORDER.desc ? true : false);
                }

                m++;
            }

            sort.setSort(sortFields);


        }


        //多线程搜素
        int shardsCount = shards.length;
        // Run the search in parallel on the shards with a thread pool.
        CompletionService<Result> csSearch = new ExecutorCompletionService<Result>(threadPool);
        String convertClass = query.get("document.convertor.class");
        //这里是并行搜索, 搜索并直接返回结果
        for (int i = 0; i < shardsCount; i++) {
            QueryCall call = new QueryCall(shards[i], offset, limit, timeout, luceneQuery, sort, fields);
            if(StringUtils.isNotBlank(convertClass)) {
                DocumentConvertor convertor = (DocumentConvertor)cache.getIfPresent(convertClass);
                if(convertor == null) {
                    convertor = ReflectionUtil.<DocumentConvertor>newInstance(convertClass);
                    cache.put(convertClass, convertor);
                }
                call.setConvertor(convertor);
            }

            csSearch.submit(call);
        }

        //查询到的总数, numFount
        AtomicInteger totalHits = new AtomicInteger(0);

        //处理搜索结果，泛型
        List docs = new LinkedList<Serializable>();
        for (int i = 0; i < shardsCount; i++) {
            try {
                Result queryResult = csSearch.take().get();

                //获取每一个shard的文档总数
                totalHits.addAndGet(queryResult.getTotalHits());
                response.setMaxScore(queryResult.getMaxScore());

                //搜索的结果, 都在这里保存着
                docs.addAll(queryResult.getDocuments());
            } catch (InterruptedException e) {
                throw new IOException("Multithread shard search interrupted:", e);
            } catch (ExecutionException e) {
                throw new IOException("Multithread shard search could not be executed:", e);
            }
        }

        response.addDocs(docs);
        //发现总数
        response.setNumFount(totalHits.get());
    }



    @Override
    public GroupResultWritable group(QueryWritable query,
                                     String[] shards,
                                     long timeout) throws IOException {
        getSearcherHandleByShard(shards[0]);
        return super.group(query, shards, timeout);
    }



    @Override
    public FacetResultWritable facet(QueryWritable query,
                                     String[] shards,
                                     long timeout) throws IOException {
        getSearcherHandleByShard(shards[0]);
        return super.facet(query, shards, timeout);
    }


    /**
     * 对数值区间，时间区间进行Facet。
     * @param query 查询语句
     * @param shards 查询分片
     * @param timeout 超时时间
     * @return 返回Facet的结果
     * @throws IOException
     */
    @Override
    public FacetResultWritable facetByRange(QueryWritable query,
                                     String[] shards,
                                     long timeout) throws IOException {
        getSearcherHandleByShard(shards[0]);
        return super.facetByRange(query, shards, timeout);
    }


}
