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
package com.ivyft.katta.lib.lucene;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.ivyft.katta.lib.lucene.collector.FetchDocumentCollector;
import com.ivyft.katta.lib.lucene.convertor.DocumentConvertor;
import com.ivyft.katta.node.IContentServer;
import com.ivyft.katta.node.SerialSocketServer;
import com.ivyft.katta.util.ClassUtil;
import com.ivyft.katta.util.NodeConfiguration;
import com.ivyft.katta.util.ReflectionUtil;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.range.Range;
import org.apache.lucene.facet.range.RangeAccumulator;
import org.apache.lucene.facet.range.RangeFacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.*;
import org.apache.lucene.search.grouping.*;
import org.apache.lucene.search.grouping.function.FunctionAllGroupsCollector;
import org.apache.lucene.search.grouping.function.FunctionFirstPassGroupingCollector;
import org.apache.lucene.search.grouping.function.FunctionSecondPassGroupingCollector;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * scoring across all the nodes / instances of LuceneServer.
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
public class LuceneServer implements IContentServer, ILuceneServer {

    /**
     * 配置的IndexSearcher Factory的工厂类
     */
    public final static String CONF_KEY_SEARCHER_FACTORY_CLASS = "lucene.searcher.factory-class";


    /**
     * 关闭 IndexSearcher 的 Policy
     */
    public final static String CONF_KEY_SEARCHER_CLOSE_POLICY_CLASS = "lucene.searcher.close.policy-class";


    /**
     * 搜索时, 超时控制
     */
    public final static String CONF_KEY_COLLECTOR_TIMOUT_PERCENTAGE = "lucene.collector.timeout-percentage";


    /**
     * 搜索线程池初始化大小
     */
    public final static String CONF_KEY_SEARCHER_THREADPOOL_CORESIZE = "lucene.searcher.threadpool.core-size";


    /**
     * 搜索线程池最大线程数
     */
    public final static String CONF_KEY_SEARCHER_THREADPOOL_MAXSIZE = "lucene.searcher.threadpool.max-size";


    /**
     * 开启缓存长度？
     */
    public final static String CACHE_MAX_NUMBER_SIZE = "lucene.cache.max-size";




    /**
     * 缓存时间（单位分钟）？
     */
    public final static String CACHE_MAX_EXPIRE_MINUTES = "lucene.cache.expire-minutes";


    /**
     * Lucene在Facet时缓存配置Key
     */
    public final static String FACET_CACHE_MB = "lucene.facet.cache.mb";



    /**
     *
     * 关闭 IndexSearcher 时, 如果 IndexSearcher 正在使用, 则等待的时间.
     */
    public static final int INDEX_HANDLE_CLOSE_SLEEP_TIME = 50;



    /**
     * 默认的SolrQuery
     */
    public final static SolrQuery DEFAULT_QUERY = new SolrQuery();



    /**
     * 该map存放所有的Seacher映射。
     *
     * shardName#IndexDirName -> IndexDirPath IndexSearcher
     */
    protected final Map<String, SearcherHandle> searcherHandlesByShard = new ConcurrentHashMap<String, SearcherHandle>();



    /**
     * 该map存放所有的Seacher映射。
     *
     * shardName#IndexDirName -> IndexDirPath IndexSearcher
     */
    protected final Map<String, SolrHandler> shardBySolrPath = new ConcurrentHashMap<String, SolrHandler>();



    /**
     * 简单的缓存
     */
    protected Cache<String, Object> cache;


    /**
     * 线程池，查询时使用
     */
    protected ExecutorService threadPool;


    /**
     * 该类的实例会结束超时的查询
     */
    protected TimeLimitingCollector.TimerThread searchTimerThread;


    /**
     * 定时扫描, 关闭长期不使用的 IndexSearcher 的内部线程
     */
    protected CloseIndexSearcherThread closeIndexSearcherThread;


    /**
     *
     * Katta 内部打开的 Hadoop Reader Port Server
     *
     */
    protected SerialSocketServer serialSocketServer;


    /**
     * 超时计数器
     */
    private Counter searchTimerCounter;


    /**
     * 当前机器名+RPC Port
     */
    protected String nodeName;




    protected float timeoutPercentage = 0.75f;


    /**
     * Lucene在Facet时缓存大小
     */
    protected double facetCacheMB = 64d;


    /**
     * 负责创建Lucene索引的Factory工厂类
     */
    protected ISeacherFactory seacherFactory;


    /**
     * 关闭长期不用的 IndexSearcher, 以释放资源
     */
    protected CloseIndexSearcherPolicy closeIndexSearcherPolicy;


    /**
     * 关闭当前 Server
     */
    protected final AtomicBoolean shutdown = new AtomicBoolean(false);



    /**
     * Log
     */
    private final static Logger LOG = LoggerFactory.getLogger(LuceneServer.class);


    /**
     * 默认构造方法必须有，用反射实例化
     *
     */
    public LuceneServer() {
        DEFAULT_QUERY.setStart(0);
        DEFAULT_QUERY.setRows(10);
        DEFAULT_QUERY.set(CommonParams.TIME_ALLOWED, 5 * 1000);
    }

    /**
     *
     * Constructor for testing purpose, {@link #init(String, NodeConfiguration)}
     * need not to be called.
     *
     * @param name
     * @param seacherFactory
     * @param timeoutPercentage
     */
    public LuceneServer(String name, ISeacherFactory seacherFactory, float timeoutPercentage) {
        DEFAULT_QUERY.setStart(0);
        DEFAULT_QUERY.setRows(10);
        DEFAULT_QUERY.set(CommonParams.TIME_ALLOWED, 5 * 1000);

        init(name, new NodeConfiguration());
        this.seacherFactory = seacherFactory;
        this.timeoutPercentage = timeoutPercentage;
    }



    @Override
    public void init(String nodeName, NodeConfiguration nodeConfiguration) {
        this.nodeName = nodeName;

        //利用反射实例化seacherFactory
        this.seacherFactory = (ISeacherFactory) ClassUtil.newInstance(
                nodeConfiguration.getClass(
                CONF_KEY_SEARCHER_FACTORY_CLASS, DefaultSearcherFactory.class));


        this.closeIndexSearcherPolicy = (CloseIndexSearcherPolicy) ClassUtil.newInstance(
                nodeConfiguration.getClass(
                        CONF_KEY_SEARCHER_CLOSE_POLICY_CLASS, DefaultCloseIndexSearcherPolicy.class));
        this.closeIndexSearcherPolicy.init(nodeConfiguration);

        this.timeoutPercentage = nodeConfiguration.getFloat(CONF_KEY_COLLECTOR_TIMOUT_PERCENTAGE, this.timeoutPercentage);

        if (this.timeoutPercentage < 0 || this.timeoutPercentage > 1) {
            throw new IllegalArgumentException("illegal value '" + this.timeoutPercentage + "' for "
                    + CONF_KEY_COLLECTOR_TIMOUT_PERCENTAGE + ". Only values between 0 and 1 are allowed.");
        }
        int coreSize = nodeConfiguration.getInt(CONF_KEY_SEARCHER_THREADPOOL_CORESIZE, 10);
        int maxSize = nodeConfiguration.getInt(CONF_KEY_SEARCHER_THREADPOOL_MAXSIZE, coreSize * 8);
        this.facetCacheMB = nodeConfiguration.getDouble(FACET_CACHE_MB, 64.0D);



        threadPool = new ThreadPoolExecutor(
                coreSize,
                maxSize,
                100L,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>());

        int filterCacheSize = nodeConfiguration.getInt(CACHE_MAX_NUMBER_SIZE, 5000);
        int filterCacheTimeMinutes = nodeConfiguration.getInt(CACHE_MAX_EXPIRE_MINUTES, 30);
        //初始化缓存
        cache = CacheBuilder.newBuilder()
                .expireAfterAccess(filterCacheTimeMinutes, TimeUnit.MINUTES)
                .maximumSize(filterCacheSize).build();


        //初始化Hadoop Adapter
        serialSocketServer = new SerialSocketServer(this, nodeConfiguration);
        serialSocketServer.setDaemon(true);
        serialSocketServer.setName("SerialSocketServerThread");
        serialSocketServer.start();

        //超时的查询后被强制结束
        searchTimerCounter = Counter.newCounter(true);
        searchTimerThread = new TimeLimitingCollector.TimerThread(searchTimerCounter);
        searchTimerThread.setDaemon(true);
        searchTimerThread.setName("TimeLimitingThread");
        searchTimerThread.start();


        int minute = nodeConfiguration.getInt("lucene.searcher.close.thread.period.minute", 5);

        closeIndexSearcherThread = new CloseIndexSearcherThread("CloseIndexSearcherThread", minute);
        closeIndexSearcherThread.setDaemon(true);
        closeIndexSearcherThread.start();

    }


    class CloseIndexSearcherThread extends Thread {


        private final int periodMinute;


        public CloseIndexSearcherThread(String name, int periodMinute) {
            super(name);
            this.periodMinute = periodMinute;
        }

        @Override
        public void run() {
            while (!shutdown.get()) {
                try {
                    LOG.info("notify close index searcher thread...");
                    for (Map.Entry<String, SearcherHandle> entry : searcherHandlesByShard.entrySet()) {
                        try {
                            entry.getValue().closeWithPolicy(entry.getKey(), LuceneServer.this.closeIndexSearcherPolicy);
                        } catch (Exception e) {
                            LOG.error("close " + entry.getKey() + " index searcher, print error: ", e);
                        }
                    }

                    synchronized (this) {
                        try {
                            //睡眠一分钟
                            this.wait(periodMinute * 60 * 1000L);
                        } catch (InterruptedException e) {
                            LOG.warn("interrupt " + this.getName() + ", @@@");
                            return;
                        }
                    }

                } catch (Exception e) {
                    LOG.warn(this.getName() + " error print: ", e);
                }
            }

            LOG.info(getName() + " exit.");
        }
    }




    /**
     * 机器名+RPC Port
     * @return
     */
    public String getNodeName() {
        return this.nodeName;
    }


    public boolean getShutdown() {
        return shutdown.get();
    }

    public float getTimeoutPercentage() {
        return this.timeoutPercentage;
    }

    public long getCollectorTiemout(long clientTimeout) {
        return (long) (this.timeoutPercentage * clientTimeout);
    }

    /**
     * Adds an shard index search for given name to the list of shards
     * MultiSearcher search in.
     *
     * @param shardName shardName#IndexDirName
     * @param shardDir IndexDirPath
     * @throws IOException
     */
    @Override
    public void addShard(final String shardName,
                         final URI shardDir,
                         final String collectionName) throws IOException {
        LOG.info("LuceneServer " + this.nodeName + " got shard " + shardName);
        try {
            searcherHandlesByShard.put(shardName,
                    new SearcherHandle(seacherFactory, collectionName, shardName, shardDir));

            LOG.info("collectionName: " + collectionName);
            SolrHandler solrHandler = new SolrHandler(shardName, collectionName);
            LOG.info("solr core is null: " + (solrHandler.getSolrCore() == null));

            shardBySolrPath.put(shardName, solrHandler);
        } catch (Exception e) {
            LOG.error("Error building index for shard " + shardName, e);
            throw new IOException(e);
        }
    }

    /**
     * Removes a search by given shardName from the list of searchers.
     * @param shardName shardName#IndexDirName
     */
    @Override
    public void removeShard(final String shardName) {
        LOG.info("LuceneServer " + this.nodeName + " removing shard " + shardName);
        SearcherHandle handle = searcherHandlesByShard.remove(shardName);
        shardBySolrPath.remove(shardName);

        if (handle != null) {
            try {
                handle.closeSearcher(shardName);
            } catch (Exception e) {
                LOG.error("LuceneServer " + this.nodeName + " error removing shard " + shardName, e);
            }
        }
    }


    /**
     *
     * 返回所有shard
     * @return
     */
    @Override
    public Collection<String> getShards() {
        return Collections.unmodifiableCollection(searcherHandlesByShard.keySet());
    }


    public Future<?> submit(Runnable runnable) {
        return threadPool.submit(runnable);
    }



    /**
     * Returns the number of documents a shard has.
     *
     * @param shardName
     * @return the number of documents in the shard.
     */
    protected int shardSize(String shardName) {
        SearcherHandle handle = getSearcherHandleByShard(shardName);
        try {
            IndexSearcher searcher = handle.getSearcher();
            if (searcher != null) {
                int size = searcher.getIndexReader().numDocs();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Shard '" + shardName + "' has " + size + " docs.");
                }
                return size;
            }
            throw new IllegalArgumentException("Shard '" + shardName + "' unknown");
        } finally {
            handle.finishSearcher();
        }
    }


    public Map<String, SearcherHandle> getSearcherHandlesByShard() {
        return searcherHandlesByShard;
    }


    public Map<String, SolrHandler> getShardBySolrPath() {
        return shardBySolrPath;
    }

    /**
     * Returns data about a shard. Currently the only standard key is
     * SHARD_SIZE_KEY. This value will be reported by the listIndexes command. The
     * units depend on the type of server. It is OK to return an empty map or
     * null.
     *
     * @param shardName The name of the shard to measure. This was the name provided in
     *                  addShard().
     * @return a map of key/value pairs which describe the shard.
     * @throws Exception
     */
    @Override
    public Map<String, String> getShardMetaData(String shardName) throws Exception {
        Map<String, String> metaData = new HashMap<String, String>();
        metaData.put(SHARD_SIZE_KEY, Integer.toString(shardSize(shardName)));
        return metaData;
    }



    /**
     * Close all Lucene indices. No further calls will be made after this one.
     */
    @Override
    public void shutdown() throws IOException {
        shutdown.set(true);
        for (Map.Entry<String, SearcherHandle> entry : searcherHandlesByShard.entrySet()) {
            SearcherHandle handle = entry.getValue();
            try {
                handle.closeSearcher(entry.getKey());
            } catch (Exception e) {
                LOG.warn(ExceptionUtils.getFullStackTrace(e));
            }
        }

        searcherHandlesByShard.clear();
        shardBySolrPath.clear();
        searchTimerThread.stopTimer();

        try {
            serialSocketServer.close();
            serialSocketServer.interrupt();
            serialSocketServer.join();
            LOG.info(serialSocketServer.getName() + " exit.");
        } catch (Exception e) {

        }

        try {
            synchronized (closeIndexSearcherThread) {
                closeIndexSearcherThread.notify();
            }
            closeIndexSearcherThread.join();
        } catch (Exception e) {

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
        if (handle == null) {
            throw new IllegalStateException("no index-server for shard '" +
                    shardName + "' found - probably undeployed");
        }
        return handle;
    }


    /**
     * Hadoop RPC 要求这个方法。 返回值代表相同版本， 不同返回值不能调用
     * @param protocol
     * @param clientVersion
     * @return 返回值代表相同版本， 不同返回值不能调用
     * @throws IOException
     */
    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return ILuceneServer.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return new ProtocolSignature(ILuceneServer.versionID, null);
    }

    @Override
    public HitsMapWritable search(QueryWritable query,
                                  String[] shardNames,
                                  long timeout)
            throws IOException {
        int count = query.getQuery().getRows() == null ? DEFAULT_QUERY.getRows() : query.getQuery().getRows();
        return search(query, shardNames, timeout, count);
    }

    @Override
    public HitsMapWritable search(QueryWritable query,
                                  String[] shards,
                                  long timeout,
                                  int count) throws IOException {

        SolrQuery solrQuery = query.getQuery();
        LOG.info("Lucene query: " + solrQuery.toString() + " shards: [" + StringUtils.join(shards, ",") +"]");

        HitsMapWritable result = new HitsMapWritable(getNodeName());

        long start = System.currentTimeMillis();

        //启动多线程搜索
        search(solrQuery, shards, result, count, timeout);

        //计时
        if (LOG.isDebugEnabled()) {
            long completeSearchTime = (System.currentTimeMillis() - start);
            LOG.debug("Complete search took " + completeSearchTime / 1000.0 + "sec.");
            DataOutputBuffer buffer = new DataOutputBuffer();
            try {
                result.write(buffer);
                LOG.debug("Result size to transfer: " + buffer.getLength());
            } catch (IOException e) {
                LOG.warn("io", e);
            }
        }
        return result;
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
        return getDetail(shards, docId, null);
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
        //所有的结果都要存放在result变量中
        MapWritable result = new MapWritable();

        //从索引中取出数据
        Document doc = doc(shards[0], docId, fieldNames);

        //i遍历所有的field
        List<IndexableField> fields = doc.getFields();
        for (IndexableField field : fields) {
            String name = field.name();

            if(field.stringValue() == null) {
               continue;
            }

            //判断各种数据类型
            if (field.fieldType().docValueType() == FieldInfo.DocValuesType.BINARY) {
                byte[] binaryValue = field.binaryValue().clone().bytes;
                result.put(new Text(name), new BytesWritable(binaryValue));
            } if (field.fieldType().docValueType() == FieldInfo.DocValuesType.NUMERIC) {
                Number value = field.numericValue();
                if(value instanceof Integer) {
                    result.put(new Text(name), new IntWritable(value.intValue()));
                } else if(value instanceof Long) {
                    result.put(new Text(name), new LongWritable(value.longValue()));
                } else if(value instanceof Double) {
                    result.put(new Text(name), new DoubleWritable(value.doubleValue()));
                } else if(value instanceof Float) {
                    result.put(new Text(name), new FloatWritable(value.floatValue()));
                } else  if(value instanceof Short) {
                    result.put(new Text(name), new IntWritable(value.intValue()));
                } else {
                    result.put(new Text(name), new IntWritable(value.intValue()));
                }
            } else {
                String stringValue = field.stringValue();
                result.put(new Text(name), new Text(stringValue));
            }
        }
        return result;
    }


    public ArrayMapWritable getDetails(String[] shards,
                                 int[] docIds) throws IOException {
        return null;
    }


    @Override
    public int count(QueryWritable query,
                              String[] shards,
                              long timeout) throws IOException {
        return search(query, shards, timeout, 1).getTotalHits();
    }


    /**
     * Search in the given shards and return max hits for given query
     *
     * @param query
     * @param shards
     * @param result
     * @param max
     * @throws IOException
     */
    protected final void search(SolrQuery query,
                                String[] shards,
                                HitsMapWritable result,
                                int max,
                                long timeout) throws IOException {
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
            SearchCall call = new SearchCall(shards[i], max, timeout, i, luceneQuery);
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
        SolrQuery solrQuery = query.getQuery();
        LOG.info("Lucene query: " + solrQuery.toString() + " shards: [" + StringUtils.join(shards, ",") +"]");

        int count = query.getQuery().getRows() == null ? DEFAULT_QUERY.getRows() : query.getQuery().getRows();

        long start = System.currentTimeMillis();

        QueryResponse response = new QueryResponse();

        //启动多线程搜索
        query(solrQuery, shards, response, count, timeout);

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
     * @param max
     * @throws IOException
     */
    protected final void query(SolrQuery query,
                                String[] shards,
                                QueryResponse response,
                                int max,
                                long timeout) throws IOException {
        timeout = getCollectorTiemout(timeout);
        Query luceneQuery;

        String fieldString = query.getFields();
        String convertClass = query.get("document.convertor.class");

        Set<String> fields = null;
        if(StringUtils.isNotBlank(fieldString)) {
            fields = new HashSet<String>(Arrays.asList(fieldString));
        }

        try {
            luceneQuery = parse(query, shards[0]);
        } catch (Exception e) {
            throw new IOException(e);
        }

        int shardsCount = shards.length;

        // Run the search in parallel on the shards with a thread pool.
        CompletionService<Result> csSearch = new ExecutorCompletionService<Result>(threadPool);

        //这里是并行搜索, 搜索并直接返回结果
        for (int i = 0; i < shardsCount; i++) {
            QueryCall call = new QueryCall(shards[i], max, timeout, luceneQuery, fields);
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

        //结果，泛型
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

        response.setDocs(docs);
        //发现总数
        response.setNumFount(totalHits.get());
    }



    @Override
    public GroupResultWritable group(QueryWritable query,
                                     String[] shards,
                                     long timeout) throws IOException {
        SolrQuery solrQuery = query.getQuery();
        LOG.info("Lucene Group Query: " + solrQuery.toString() + " shards: [" + StringUtils.join(shards, ",") +"]");

        long start = System.currentTimeMillis();

        //启动多线程搜索
        GroupResultWritable response = group(solrQuery, shards, timeout);

        //计时
        LOG.debug("Complete search took " + (System.currentTimeMillis() - start) + "ms.");
        return response;
    }



    /**
     * Search in the given shards and return max hits for given query
     *
     * @param query
     * @param shards
     * @throws IOException
     */
    protected final GroupResultWritable<Writable> group(SolrQuery query,
                               String[] shards,
                               long timeout) throws IOException {
        timeout = getCollectorTiemout(timeout);
        Query luceneQuery;

        String groupField = query.getFacetFields()[0];

        try {
            luceneQuery = parse(query, shards[0]);
        } catch (Exception e) {
            throw new IOException(e);
        }

        int shardsCount = shards.length;

        //TODO 这一步，确定统计区间类型
        TypeSource typeSource = shardBySolrPath.get(shards[0]).getFieldGroup(groupField);

        GroupResultWritable<Writable> response = new GroupResultWritable(typeSource.getSimpleValue());
        // Run the search in parallel on the shards with a thread pool.
        CompletionService<Group<Writable>> csSearch = new ExecutorCompletionService<Group<Writable>>(threadPool);

        //这里是并行搜索, 搜索并直接返回结果
        for (int i = 0; i < shardsCount; i++) {
            GroupCall<Writable> call = new GroupCall<Writable>(shards[i], groupField,
                    timeout, luceneQuery, typeSource);
            csSearch.submit(call);
        }

        for (int i = 0; i < shardsCount; i++) {
            try {
                Group<Writable> groupResult = csSearch.take().get();
                response.setMaxScore(groupResult.getMaxScore());
                response.addTotalHitCount(groupResult.getTotalHitCount());
                response.addAll(groupResult.groupResult);
            } catch (InterruptedException e) {
                throw new IOException("Multithread shard search interrupted:", e);
            } catch (ExecutionException e) {
                e.printStackTrace();
                throw new IOException("Multithread shard search could not be executed:", e);
            }
        }

        return response;
    }



    @Override
    public FacetResultWritable facet(QueryWritable query,
                                     String[] shards,
                                     long timeout) throws IOException {
        SolrQuery solrQuery = query.getQuery();
        LOG.info("Lucene Facet Query: " + solrQuery.toString() + " shards: [" + StringUtils.join(shards, ",") +"]");

        long start = System.currentTimeMillis();

        //启动多线程搜索
        FacetResultWritable response = facet(solrQuery, shards, timeout);

        //计时
        LOG.debug("Complete search took " + (System.currentTimeMillis() - start) + "ms.");
        return response;
    }


    /**
     * Search in the given shards and return max hits for given query
     *
     * @param query
     * @param shards
     * @throws IOException
     */
    protected final FacetResultWritable<Writable> facet(SolrQuery query,
                                                        String[] shards,
                                                        long timeout) throws IOException {
        timeout = getCollectorTiemout(timeout);
        Query luceneQuery;

        String groupField = query.getFacetFields()[0];

        try {
            luceneQuery = parse(query, shards[0]);
        } catch (Exception e) {
            throw new IOException(e);
        }

        int shardsCount = shards.length;
        int facetLimit = query.getFacetLimit();


        TypeSource typeSource = shardBySolrPath.get(shards[0]).getFieldGroup(groupField);

        FacetResultWritable<Writable> response = new FacetResultWritable<Writable>(typeSource.getSimpleValue());
        // Run the search in parallel on the shards with a thread pool.
        CompletionService<Facet<Writable>> csSearch = new ExecutorCompletionService<Facet<Writable>>(threadPool);

        //这里是并行搜索, 搜索并直接返回结果
        for (int i = 0; i < shardsCount; i++) {
            FacetV2Call<Writable> call = new FacetV2Call<Writable>(shards[i],
                    groupField, facetLimit,
                    timeout, luceneQuery, typeSource);
            csSearch.submit(call);
        }

        for (int i = 0; i < shardsCount; i++) {
            try {
                Facet<Writable> facetResult = csSearch.take().get();
                response.addAll(facetResult.facetResult);
                response.setMaxScore(facetResult.getMaxScore());
                response.addTotalHitCount(facetResult.getTotalHitCount());
                response.addTotalGroupedHitCount(facetResult.getTotalGroupedHitCount());
            } catch (InterruptedException e) {
                throw new IOException("Multithread shard search interrupted:", e);
            } catch (ExecutionException e) {
                throw new IOException("Multithread shard search could not be executed:", e);
            }
        }

        return response;
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
        SolrQuery solrQuery = query.getQuery();
        LOG.info("Lucene Facet Query: " + solrQuery.toString() + " shards: [" + StringUtils.join(shards, ",") +"]");

        long start = System.currentTimeMillis();

        //启动多线程搜索
        FacetResultWritable response = facetByRange(solrQuery, shards, timeout);

        //计时
        LOG.debug("Complete search took " + (System.currentTimeMillis() - start) + "ms.");
        return response;
    }


    /**
     * Facet Number Field OR Date Field
     * @param solrQuery solr query
     * @param shards shard
     * @param timeout timeout
     * @return Facet Result
     * @throws IOException
     */
    private FacetResultWritable facetByRange(SolrQuery solrQuery,
                                     String[] shards,
                                     long timeout) throws IOException {
        timeout = getCollectorTiemout(timeout);
        String facetField = solrQuery.getFacetFields()[0];
        if(StringUtils.isBlank(facetField)) {
            throw new IllegalArgumentException("facet field must is not blank, method SolrQuery.addFacetFields(...)");
        }
        SolrHandler handler = shardBySolrPath.get(shards[0]);

        TypeSource typeSource = handler.getFieldGroup(facetField);
        if(typeSource.getSimpleValue() instanceof Text) {
            throw new IllegalArgumentException(facetField + " is not a number field.");
        }

        List<Range> ranges = null;
        if(handler.isDate(solrQuery, facetField, typeSource.getSchemaField())){
            String facetDate = solrQuery.get(FacetParams.FACET_DATE);
            if(facetDate == null) {
                facetDate = facetField;
            }
            if(handler.isDateStep(solrQuery, facetDate)) {
                String s = solrQuery.get(FacetParams.FACET_DATE_START); //初始值
                String e = solrQuery.get(FacetParams.FACET_DATE_END);   //结束值
                String g = solrQuery.get(FacetParams.FACET_DATE_GAP);   //步增区间

                ranges = typeSource.getCollectorFactory().getDateRanges(facetDate, s, e, g);
            } else {
                String[] dateRange = solrQuery.getParams(FacetParams.FACET_QUERY); //初始值
                if(dateRange == null) {
                    throw new IllegalArgumentException("facet date type field: " +
                            facetDate + "，but not found " +
                            FacetParams.FACET_DATE_START + "/" + FacetParams.FACET_DATE_END
                            + "/" + FacetParams.FACET_DATE_GAP +
                            " or " + FacetParams.FACET_QUERY);
                }
                ranges = typeSource.getCollectorFactory().getDateRanges(facetDate, dateRange);
            }
        } else {
            String facetNumber = solrQuery.get(FacetParams.FACET_RANGE);
            if(facetNumber == null) {
                facetNumber = facetField;
            }
            if(handler.isNumberStep(solrQuery, facetNumber)) {
                String s = solrQuery.get(FacetParams.FACET_RANGE_START); //初始值
                String e = solrQuery.get(FacetParams.FACET_RANGE_END);   //结束值
                String g = solrQuery.get(FacetParams.FACET_RANGE_GAP);   //步增区间

                ranges = typeSource.getCollectorFactory().getNumberRanges(facetNumber, s, e, g);
            } else {
                String[] numberRange = solrQuery.getParams(FacetParams.FACET_QUERY);
                if(numberRange == null) {
                    throw new IllegalArgumentException("facet number type field: " +
                            facetNumber +
                            "，but not found " +
                            FacetParams.FACET_RANGE_START + "/" + FacetParams.FACET_RANGE_END
                            + "/" + FacetParams.FACET_RANGE_GAP +
                            " or " + FacetParams.FACET_QUERY);
                }
                ranges = typeSource.getCollectorFactory().getNumberRanges(facetNumber, numberRange);
            }
        }

        if(ranges.isEmpty()) {
            throw new IllegalArgumentException(facetField + " 不能解析有效的区间。");
        }

        Query luceneQuery;

        try {
            luceneQuery = parse(solrQuery, shards[0]);
        } catch (Exception e) {
            throw new IOException(e);
        }
        int facetMinCount = solrQuery.getInt(FacetParams.FACET_MINCOUNT, 0);

        int shardsCount = shards.length;
        //启动多线程搜索
        FacetResultWritable<Writable> response = new FacetResultWritable(
                typeSource.getSimpleValue());
        // Run the search in parallel on the shards with a thread pool.
        CompletionService<Facet<Writable>> csSearch = new ExecutorCompletionService<Facet<Writable>>(threadPool);

        //这里是并行搜索, 搜索并直接返回结果
        for (int i = 0; i < shardsCount; i++) {
            FacetRangeCall<Writable> call =
                    new FacetRangeCall<Writable>(shards[i],
                            facetField,
                            timeout,
                            luceneQuery,
                            facetMinCount,
                            ranges,
                            typeSource);
            csSearch.submit(call);
        }

        for (int i = 0; i < shardsCount; i++) {
            try {
                Facet<Writable> facetResult = csSearch.take().get();
                response.addAll(facetResult.facetResult);
            } catch (InterruptedException e) {
                throw new IOException("Multithread shard search interrupted:", e);
            } catch (ExecutionException e) {
                throw new IOException("Multithread shard search could not be executed:", e);
            }
        }
        return response;
    }


    /**
     * 使用Solr来解析Lucene的Query
     * @param solrQuery Solr Query
     * @param shardName shardName
     * @return
     * @throws SyntaxError
     */
    protected Query parse(SolrQuery solrQuery, String shardName) throws SyntaxError {
        SolrHandler handler = shardBySolrPath.get(shardName);

        LOG.debug("solr core is null: " + (handler.getSolrCore() == null));

        LocalSolrQueryRequest request = new LocalSolrQueryRequest(handler.getSolrCore(), DEFAULT_QUERY);

        SolrPluginUtils.setDefaults(request, DEFAULT_QUERY, solrQuery, null);

        String q = solrQuery.getQuery();
        String[] fq = solrQuery.getFilterQueries();

        boolean b1 = StringUtils.isNotBlank(q);
        boolean b2 = ArrayUtils.isNotEmpty(fq);
        //重新定义一个数组, 把q和qs放到一起
        String[] queryStrings = null;
        if(b1 && b2) {
            //重新定义一个数组, 把q和qs放到一起
            queryStrings = new String[fq.length + 1];

            queryStrings[0] = q;
            //这里在复制数组, 一定要小心.
            System.arraycopy(fq, 0, queryStrings, 1, fq.length);
        } else if (b1) {
            queryStrings = new String[]{q};
        } else if(b2) {
            queryStrings = fq;
        } else {
            //q和fq都为null的情况. 直接抛出异常
            throw new IllegalArgumentException("q or fq must not null.");
        }

        List<Query> queries = SolrPluginUtils.parseQueryStrings(request, queryStrings);

        //把所有的Query用BooleanQuery累积到一起.他们是and的关系
        BooleanQuery booleanQuery = new BooleanQuery();
        for (Query qx : queries) {
            booleanQuery.add(qx, BooleanClause.Occur.MUST);
        }

        return booleanQuery;
    }

    /**
     * Merges the already sorted sub-lists to one big sorted list.
     */
    private final static List<Hit> mergeFieldSort(FieldSortComparator comparator,
                                                  int count,
                                                  ScoreDoc[][] sortedFieldDocs,
                                                  String[] shards,
                                                  String nodeName) {
        int[] arrayPositions = new int[sortedFieldDocs.length];
        List<Hit> sortedResult = new ArrayList<Hit>(count);

        BitSet listDone = new BitSet(sortedFieldDocs.length);
        for (int subListIndex = 0; subListIndex < arrayPositions.length; subListIndex++) {
            if (sortedFieldDocs[subListIndex].length == 0) {
                listDone.set(subListIndex, true);
            }
        }
        do {
            int fieldDocArrayWithSmallestFieldDoc = -1;
            FieldDoc smallestFieldDoc = null;
            for (int subListIndex = 0; subListIndex < arrayPositions.length; subListIndex++) {
                if (!listDone.get(subListIndex)) {
                    FieldDoc hit = (FieldDoc) sortedFieldDocs[subListIndex][arrayPositions[subListIndex]];
                    if (smallestFieldDoc == null || comparator.compare(hit.fields, smallestFieldDoc.fields) < 0) {
                        smallestFieldDoc = hit;
                        fieldDocArrayWithSmallestFieldDoc = subListIndex;
                    }
                }
            }
            ScoreDoc[] smallestElementList = sortedFieldDocs[fieldDocArrayWithSmallestFieldDoc];
            FieldDoc fieldDoc = (FieldDoc) smallestElementList[arrayPositions[fieldDocArrayWithSmallestFieldDoc]];
            arrayPositions[fieldDocArrayWithSmallestFieldDoc]++;
            Hit hit = new Hit(shards[fieldDocArrayWithSmallestFieldDoc], nodeName, fieldDoc.score, fieldDoc.doc);
            sortedResult.add(hit);
            if (arrayPositions[fieldDocArrayWithSmallestFieldDoc] >= smallestElementList.length) {
                listDone.set(fieldDocArrayWithSmallestFieldDoc, true);
            }
        } while (sortedResult.size() < count && listDone.cardinality() < arrayPositions.length);
        return sortedResult;
    }

    /**
     * Returns a specified lucene document from a given shard where all or only
     * the given fields are loaded from the index.
     *
     * @param shardName
     * @param docId
     * @param fieldNames
     * @return
     * @throws IOException
     */
    protected Document doc(String shardName,
                           int docId,
                           String[] fieldNames) throws IOException {
        SearcherHandle handle = getSearcherHandleByShard(shardName);
        try {
            IndexSearcher searcher = handle.getSearcher();
            if (searcher != null) {
                if (fieldNames == null) {
                    return searcher.doc(docId);
                } else {
                    return searcher.doc(docId,
                            new HashSet<String>(Arrays.asList(fieldNames)));
                }
            }
        } finally {
            handle.finishSearcher();
        }
        return null;
    }



    /**
     * Returns a specified lucene document from a given shard where all or only
     * the given fields are loaded from the index.
     *
     * @param shardName
     * @param docIds
     * @param fieldNames
     * @return
     * @throws IOException
     */
    protected List<Document> docs(String shardName,
                           int[] docIds,
                           String[] fieldNames) throws IOException {
        SearcherHandle handle = getSearcherHandleByShard(shardName);
        try {
            IndexSearcher searcher = handle.getSearcher();
            if (searcher != null) {
                if (fieldNames == null) {
                    List<Document> docs = new LinkedList<Document>();
                    for (int docId : docIds) {
                        docs.add(searcher.doc(docId));
                    }
                    return docs;
                } else {
                    List<Document> docs = new LinkedList<Document>();
                    HashSet fields = new HashSet<String>(Arrays.asList(fieldNames));
                    for (int docId : docIds) {
                        docs.add(searcher.doc(docId, fields));
                    }
                    return docs;
                }
            }
        } finally {
            handle.finishSearcher();
        }
        return null;
    }



    /**
     *
     * Implements a single thread of a search. Each shard has a separate
     * SearchCall and they are run more or less in parallel.
     *
     *
     * @author ZhenQin
     *
     */
    protected class SearchCall implements Callable<SearchResult> {

        protected String shardName;
        protected int limit;
        protected long timeout;
        protected int callIndex;
        protected Query query;


        protected Sort sort;
        protected Filter filter;

        public SearchCall(String shardName,
                          int limit,
                          long timeout,
                          int callIndex,
                          Query query) {
            this.shardName = shardName;
            this.limit = limit;
            //_sort = sort;
            this.timeout = timeout;
            this.callIndex = callIndex;
            this.query = query;
            //_filter = filter;
        }

        @Override
        public SearchResult call() throws Exception {
            SearcherHandle handle = getSearcherHandleByShard(shardName);
            try {
                IndexSearcher searcher = handle.getSearcher();
                if (searcher == null) {
                    LOG.warn(String.format("Search attempt for shard %s skipped because shard was closed; empty result returned",
                            shardName));
                    // return empty result...
                    return new SearchResult(0, new ScoreDoc[0], callIndex, 0L);
                }

                if (limit <= 0) {
                    return new SearchResult(limit, new ScoreDoc[0], callIndex, 0L);
                }

                //TODO 这里搜索，不携带结果。但是Filter，Sort没有使用
                long time = System.currentTimeMillis();
                TopScoreDocCollector docCollector = TopScoreDocCollector.create(limit, true);
                if(filter == null) {
                    searcher.search(query, wrapInTimeoutCollector(docCollector));
                    return new SearchResult(docCollector.getTotalHits(),
                            docCollector.topDocs().scoreDocs,
                            callIndex,
                            System.currentTimeMillis() - time);
                } else {
                    searcher.search(query, filter, wrapInTimeoutCollector(docCollector));
                    return new SearchResult(docCollector.getTotalHits(),
                            docCollector.topDocs().scoreDocs,
                            callIndex,
                            System.currentTimeMillis() - time);
                }
            } finally {
                handle.finishSearcher();
            }
        }


        private Collector wrapInTimeoutCollector(TopDocsCollector resultCollector) {
            if (timeout <= 0) {
                return resultCollector;
            }

            TimeLimitingCollector timeoutCollector = new TimeLimitingCollector(resultCollector,
                    searchTimerCounter, timeout);
            timeoutCollector.setBaseline();
            return timeoutCollector;
        }
    }




    /**
     *
     * Implements a single thread of a search. Each shard has a separate
     * SearchCall and they are run more or less in parallel.
     *
     *
     * @author ZhenQin
     *
     */
    protected class QueryCall implements Callable<Result> {

        protected String shardName;
        protected int limit;
        protected long timeout;
        protected Query query;

        protected Sort sort;
        protected Set<String> fields;


        protected DocumentConvertor convertor;

        public QueryCall(String shardName,
                          int limit,
                          long timeout,
                          Query query, Set<String> fields) {
            this.shardName = shardName;
            this.limit = limit;
            //this.sort = sort;
            this.timeout = timeout;
            this.query = query;
            this.fields = fields;
        }

        @Override
        public Result call() throws Exception {
            SearcherHandle handle = getSearcherHandleByShard(shardName);
            try {
                IndexSearcher searcher = handle.getSearcher();
                if (searcher == null) {
                    LOG.warn(String.format("Search attempt for shard %s skipped because shard was closed; empty result returned",
                            shardName));
                    // return empty result...
                    return new Result(0, Collections.emptyList(), 0L, 0.0f);
                }

                if (limit <= 0) {
                    return new Result(limit, Collections.emptyList(), 0L, 0.0f);
                }

                //这里搜索，并且携带结果返回
                long time = System.currentTimeMillis();

                FetchDocumentCollector documentCollector = new FetchDocumentCollector(fields, limit, 0);
                TopScoreDocCollector topDocsCollector = TopScoreDocCollector.create(1, true);

                if(convertor != null) {
                    documentCollector.setConvertor(convertor);
                }

                searcher.search(query, MultiCollector.wrap(wrapInTimeoutCollector(documentCollector), topDocsCollector));

                return new Result(documentCollector.getTotalHits(),
                        documentCollector.getDocs(),
                        System.currentTimeMillis() - time,
                        topDocsCollector.topDocs().getMaxScore());
            } finally {
                handle.finishSearcher();
            }
        }


        private Collector wrapInTimeoutCollector(Collector resultCollector) {
            if (timeout <= 0) {
                return resultCollector;
            }

            TimeLimitingCollector timeoutCollector = new TimeLimitingCollector(resultCollector,
                    searchTimerCounter, timeout);
            timeoutCollector.setBaseline();
            return timeoutCollector;
        }


        public DocumentConvertor getConvertor() {
            return convertor;
        }

        public void setConvertor(DocumentConvertor convertor) {
            this.convertor = convertor;
        }
    }



    /**
     *
     * Implements a single thread of a search. Each shard has a separate
     * SearchCall and they are run more or less in parallel.
     *
     *
     * @author ZhenQin
     *
     */
    protected class GroupCall<T extends Writable> implements Callable<Group<T>> {

        protected String shardName;
        protected long timeout;
        protected Query query;
        protected String groupByField;
        protected TypeSource typeSource;

        public GroupCall(String shardName,
                         String groupByField,
                         long timeout,
                         Query query,
                         TypeSource typeSource) {
            this.shardName = shardName;
            this.groupByField = groupByField;
            this.timeout = timeout;
            this.query = query;
            this.typeSource = typeSource;
        }


        @Override
        public Group<T> call() throws Exception {
            SearcherHandle handle = getSearcherHandleByShard(shardName);
            try {
                IndexSearcher searcher = handle.getSearcher();
                if (searcher == null) {
                    LOG.warn(String.format("Search attempt for shard %s " +
                            "skipped because shard was closed; empty result returned",
                            shardName));
                    // return empty result...
                    return new Group<T>(shardName, 0);
                }

                //这里搜索，并且携带结果返回
                long time = System.currentTimeMillis();

                Map<String, Object> map = ValueSource.newContext(searcher);

                FunctionAllGroupsCollector allGroupsCollector = new FunctionAllGroupsCollector(
                        typeSource.getFieldCacheSource(), map);
                TopScoreDocCollector topDocsCollector = TopScoreDocCollector.create(1, true);


                searcher.search(query, wrapInTimeoutCollector(MultiCollector.wrap(topDocsCollector, allGroupsCollector)));

                Collection<MutableValue> topGroups = allGroupsCollector.getGroups();

                Group group = new Group<T>(shardName, (System.currentTimeMillis() - time));
                group.setMaxScore(topDocsCollector.topDocs().getMaxScore());
                group.setTotalHitCount(topDocsCollector.getTotalHits());

                for (MutableValue v : topGroups) {
                    group.add(typeSource.getCollectorFactory().convert(v.toObject()));
                }
                return group;
            } finally {
                handle.finishSearcher();
            }
        }


        private Collector wrapInTimeoutCollector(Collector resultCollector) {
            if (timeout <= 0) {
                return resultCollector;
            }

            TimeLimitingCollector timeoutCollector = new TimeLimitingCollector(resultCollector,
                    searchTimerCounter, timeout);
            timeoutCollector.setBaseline();
            return timeoutCollector;
        }
    }



    /**
     *
     * Implements a single thread of a search. Each shard has a separate
     * SearchCall and they are run more or less in parallel.
     *
     *
     * @author ZhenQin
     *
     */
    protected class FacetCall<T extends Writable> implements Callable<Facet<T>> {

        protected String shardName;
        protected long timeout;
        protected Query query;
        protected String facetField;
        protected TypeSource typeSource;
        private int facetLimit;

        public FacetCall(String shardName,
                         String facetField,
                         int facetLimit,
                         long timeout,
                         Query query,
                         TypeSource typeSource) {
            this.shardName = shardName;
            this.facetField = facetField;
            this.facetLimit = facetLimit;
            this.timeout = timeout;
            this.query = query;
            this.typeSource = typeSource;
        }


        @Override
        public Facet<T> call() throws Exception {
            SearcherHandle handle = getSearcherHandleByShard(shardName);
            try {
                IndexSearcher searcher = handle.getSearcher();
                if (searcher == null) {
                    LOG.warn(String.format("Search attempt for shard %s " +
                            "skipped because shard was closed; empty result returned",
                            shardName));
                    // return empty result...
                    return new Facet<T>(shardName, 0);
                }

                //这里搜索，并且携带结果返回
                long time = System.currentTimeMillis();

                Map<String, Object> map = ValueSource.newContext(searcher);

                //TODO Facet下一步支持区域统计。目前缓存方面设计不够合理
                GroupingSearch groupingSearch = new GroupingSearch(
                        typeSource.getFieldCacheSource(), map);

                //groupingSearch.setGroupSort(new Sort(new SortField("floor", SortField.Type.INT)));
                groupingSearch.setFillSortFields(true);

                // Sets cache in MB
                groupingSearch.setCachingInMB(facetCacheMB, true);
                groupingSearch.setAllGroups(true);
                groupingSearch.setGroupDocsOffset(0);
                groupingSearch.setGroupDocsLimit(this.facetLimit);

                TopGroups topGroups = groupingSearch.search(searcher, query,
                        0, this.facetLimit);

                // Render groupsResult...
                Facet facet = new Facet<T>(shardName, (System.currentTimeMillis() - time));
                facet.setMaxScore(topGroups.maxScore);
                facet.setTotalHitCount(topGroups.totalHitCount);
                facet.setTotalGroupCount(topGroups.totalGroupCount);
                facet.setTotalGroupedHitCount(topGroups.totalGroupedHitCount);

                for(GroupDocs<?> doc : topGroups.groups) {
                    if(doc.groupValue instanceof MutableValue) {
                        MutableValue value = (MutableValue)doc.groupValue;
                        facet.add(typeSource.getCollectorFactory().convert(value.toObject()), doc.totalHits);
                    } else {
                        LOG.error("type error!");
                    }
                }
                return facet;
            } finally {
                handle.finishSearcher();
            }
        }
    }


    /**
     *
     * Implements a single thread of a search. Each shard has a separate
     * SearchCall and they are run more or less in parallel.
     *
     *
     * @author ZhenQin
     *
     */
    protected class FacetV2Call<T extends Writable> implements Callable<Facet<T>> {

        protected String shardName;
        protected long timeout;
        protected Query query;
        protected String facetField;
        protected TypeSource typeSource;
        private int facetLimit;

        public FacetV2Call(String shardName,
                         String facetField,
                         int facetLimit,
                         long timeout,
                         Query query,
                         TypeSource typeSource) {
            this.shardName = shardName;
            this.facetField = facetField;
            this.facetLimit = facetLimit;
            this.timeout = timeout;
            this.query = query;
            this.typeSource = typeSource;
        }


        @Override
        public Facet<T> call() throws Exception {
            SearcherHandle handle = getSearcherHandleByShard(shardName);
            try {
                IndexSearcher searcher = handle.getSearcher();
                if (searcher == null) {
                    LOG.warn(String.format("Search attempt for shard %s " +
                            "skipped because shard was closed; empty result returned",
                            shardName));
                    // return empty result...
                    return new Facet<T>(shardName, 0);
                }

                //这里搜索，并且携带结果返回
                long time = System.currentTimeMillis();

                Map map = ValueSource.newContext(searcher);

                AbstractFirstPassGroupingCollector<MutableValue> firstPassCollector =
                        new FunctionFirstPassGroupingCollector(
                                typeSource.getFieldCacheSource(),
                                map,
                                Sort.RELEVANCE, this.facetLimit);

                AbstractAllGroupsCollector allGroupsCollector =
                        new FunctionAllGroupsCollector(
                                typeSource.getFieldCacheSource(), map);

                CachingCollector cachedCollector = CachingCollector.create(
                        MultiCollector.wrap(firstPassCollector, allGroupsCollector),
                        true, facetCacheMB);

                searcher.search(query, wrapInTimeoutCollector(cachedCollector));

                AbstractSecondPassGroupingCollector<MutableValue> secondPassCollector =
                        new FunctionSecondPassGroupingCollector(
                                firstPassCollector.getTopGroups(0, true),
                                Sort.RELEVANCE, null,
                                3, true, true, true,
                                typeSource.getFieldCacheSource(), map);

                if(cachedCollector.isCached()) {
                    cachedCollector.replay(secondPassCollector);
                }

                TopGroups<MutableValue> topGroups = new TopGroups<MutableValue>(secondPassCollector.getTopGroups(0),
                        allGroupsCollector.getGroups().size());

                // Render groupsResult...
                Facet<T> facet = new Facet<T>(shardName, (System.currentTimeMillis() - time));
                facet.setMaxScore(topGroups.maxScore);
                facet.setTotalHitCount(topGroups.totalHitCount);
                facet.setTotalGroupCount(topGroups.totalGroupCount);
                facet.setTotalGroupedHitCount(topGroups.totalGroupedHitCount);

                for(GroupDocs<?> doc : topGroups.groups) {
                    if(doc.groupValue instanceof MutableValue) {
                        MutableValue value = (MutableValue)doc.groupValue;
                        facet.add(typeSource.getCollectorFactory().<T>convert(value.toObject()), doc.totalHits);
                    } else {
                        LOG.error("type error!");
                    }
                }
                return facet;
            } finally {
                handle.finishSearcher();
            }
        }

        private Collector wrapInTimeoutCollector(Collector resultCollector) {
            if (timeout <= 0) {
                return resultCollector;
            }

            TimeLimitingCollector timeoutCollector = new TimeLimitingCollector(resultCollector,
                    searchTimerCounter, timeout);
            timeoutCollector.setBaseline();
            return timeoutCollector;
        }
    }


    /**
     *
     * Implements a single thread of a search. Each shard has a separate
     * SearchCall and they are run more or less in parallel.
     *
     *
     * @author ZhenQin
     *
     */
    protected class FacetRangeCall<T extends Writable> implements Callable<Facet<T>> {

        protected String shardName;
        protected long timeout;
        protected Query query;
        protected String facetField;
        protected int facetMinCount = 0;
        protected List<Range> ranges = null;
        protected TypeSource typeSource;

        public FacetRangeCall(String shardName,
                         String facetField,
                         long timeout,
                         Query query,
                         int facetMinCount,
                         List<Range> ranges,
                         TypeSource typeSource) {
            this.shardName = shardName;
            this.facetField = facetField;
            this.timeout = timeout;
            this.query = query;
            this.facetMinCount = facetMinCount;
            this.ranges = ranges;
            this.typeSource = typeSource;
        }


        @Override
        public Facet<T> call() throws Exception {
            SearcherHandle handle = getSearcherHandleByShard(shardName);
            try {
                IndexSearcher searcher = handle.getSearcher();
                if (searcher == null) {
                    LOG.warn(String.format("Search attempt for shard %s " +
                            "skipped because shard was closed; empty result returned",
                            shardName));
                    // return empty result...
                    return new Facet<T>(shardName, 0);
                }

                //这里搜索，并且携带结果返回
                long time = System.currentTimeMillis();

                RangeFacetRequest<Range> rangeFacetRequest =
                        new RangeFacetRequest<Range>(facetField,
                                typeSource.getFieldCacheSource(), ranges);
                RangeAccumulator rangeAccumulator = new RangeAccumulator(rangeFacetRequest);

                FacetsCollector facetsCollector = FacetsCollector.create(rangeAccumulator);

                // perform documents search and facets accumulation
                searcher.search(query, wrapInTimeoutCollector(facetsCollector));

                // Obtain facets results and print them
                List<FacetResult> res = facetsCollector.getFacetResults();

                Facet facet = new Facet<T>(shardName, System.currentTimeMillis() - time);
                for (FacetResult facetResult : res) {
                    List<FacetResultNode> node = facetResult.getFacetResultNode().subResults;
                    for (FacetResultNode resultNode : node) {
                        if(resultNode.value >= facetMinCount) {
                            facet.add(typeSource.getCollectorFactory().convert(resultNode.label.components[1]),
                                    (int)resultNode.value);
                        }
                    }
                }
                return facet;
            } finally {
                handle.finishSearcher();
            }
        }



        private Collector wrapInTimeoutCollector(Collector resultCollector) {
            if (timeout <= 0) {
                return resultCollector;
            }

            TimeLimitingCollector timeoutCollector = new TimeLimitingCollector(resultCollector,
                    searchTimerCounter, timeout);
            timeoutCollector.setBaseline();
            return timeoutCollector;
        }
    }
}
