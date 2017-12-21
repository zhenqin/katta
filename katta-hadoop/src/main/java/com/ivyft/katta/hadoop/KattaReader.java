package com.ivyft.katta.hadoop;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 14-4-1
 * Time: 下午3:15
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaReader {



    public static interface DocumentCallback {


        /**
         * 一次处理每条Document
         * @param document
         */
        public void callback(SolrDocument document);


        /**
         * 处理进度
         * @param process 是一个0-1之间小数。
         */
        public void progress(float process);
    }



    /**
     * Solr Query
     */
    protected final SolrQuery query;


    /**
     * 导出查询的Index Name
     */
    protected final String[] indexNames;




    /**
     * 多线程的线程池，建议大小为最少Shard的数量
     */
    protected final ExecutorService executor;


    /**
     * Katta Split
     */
    protected final List<KattaInputSplit> splits;


    /**
     * Solr Cloud Reader
     * @param indexNames
     * @param query
     * @param executor
     * @return
     * @throws Exception
     */
    public static KattaReader getInstance(String[] indexNames,
                                          SolrQuery query,
                                          Configuration conf,
                                          String zkServers,
                                          String keyField,
                                          int batchSize,
                                          ExecutorService executor) throws Exception {

        if(batchSize <= 0) {
            batchSize = 5000;
        }

        if(StringUtils.isBlank(keyField)) {
            throw new IllegalArgumentException("input key must not be blank.");
        }
        KattaInputFormat.setZookeeperServers(conf, zkServers);
        KattaInputFormat.setInputKey(conf, keyField);
        KattaInputFormat.setLimit(conf, batchSize);
        KattaInputFormat.setInputQuery(conf, query);
        KattaInputFormat.setIndexNames(conf, indexNames);
        List<KattaInputSplit> splits = KattaSpliter.calculateSplits(conf);
        return new KattaReader(indexNames, query, splits, executor);
    }


    /**
     * Solr Cloud Reader
     * @param indexNames 索引名称
     * @param query 查询 Solr
     * @param host 索引所在 Host
     * @param port 索引所在 Host 的打开端口号
     * @param inputKey 返回值 Key 使用哪个 Field
     * @param batchSize 每个批次烦恼会的数量
     * @param executor 线程池
     * @return
     * @throws Exception
     */
    public static KattaReader getSingleKattaInstance(String[] indexNames,
                                                     SolrQuery query,
                                                     String host,
                                                     int port,
                                                     String inputKey,
                                                     int batchSize,
                                                     ExecutorService executor) throws Exception {
        return getSingleKattaInstance(indexNames,
                query,
                host,
                port,
                inputKey,
                batchSize,
                0,
                Integer.MAX_VALUE,
                executor);
    }



    /**
     * Solr Cloud Reader
     * @param indexNames 索引名称
     * @param query 查询 Solr
     * @param host 索引所在 Host
     * @param port 索引所在 Host 的打开端口号
     * @param inputKey 返回值 Key 使用哪个 Field
     * @param batchSize 每个批次烦恼会的数量
     * @param start 从哪个点开始读取
     * @param maxDocs 读取最多的数据量
     * @param executor 线程池
     * @return
     * @throws Exception
     */
    public static KattaReader getSingleKattaInstance(String[] indexNames,
                                                     SolrQuery query,
                                                     String host,
                                                     int port,
                                                     String inputKey,
                                                     int batchSize,
                                                     int start,
                                                     int maxDocs,
                                                     ExecutorService executor) throws Exception {
        List<KattaInputSplit> splits = new ArrayList<KattaInputSplit>(indexNames.length);
        for (String name : indexNames) {
            KattaInputSplit split = new KattaInputSplit(host, port, name, query, inputKey, batchSize);
            split.setStart(start);
            split.setMaxDocs(maxDocs);
            splits.add(split);
        }

        return new KattaReader(indexNames, query, splits, executor);
    }


    protected KattaReader(String[] indexNames, SolrQuery query,
                          List<KattaInputSplit> splits,
                          ExecutorService executor) {
        this.indexNames = indexNames;
        this.query = query;
        this.splits = splits;
        this.executor = executor;
    }


    /**
     * 处理Solr Cloud
     * @param callback
     */
    public void processKatta(final DocumentCallback callback) {
        if(executor == null) {
            throw new IllegalStateException("executor must not null");
        }

        if(splits.isEmpty()) {
            return;
        }

        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch downLatch = new CountDownLatch(splits.size());
        final float[] processes = new float[splits.size()];
        final AtomicInteger index = new AtomicInteger(0);
        for (final KattaInputSplit split : splits) {
            Runnable runnable = new Runnable(){

                final protected int inde = index.get();

                @Override
                public void run() {
                    KattaSocketReader socketReader = null;
                    try {
                        socketReader = new KattaSocketReader(split);
                        socketReader.initialize(split);
                        while (socketReader.nextKeyValue()) {
                            count.incrementAndGet();
                            processes[inde] = socketReader.getProgress();
                            callback.callback(socketReader.getCurrentValue());
                            if(count.get() % 200 == 0) {
                                float sum = 0.0f;
                                for (float process : processes) {
                                    sum += process;
                                }
                                callback.progress(sum / processes.length);
                            }
                        }
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    } finally {
                        downLatch.countDown();
                        if(socketReader != null) {
                            try {
                                socketReader.close();
                            } catch (Exception e) {

                            }
                        }
                    }
                }
            };
            index.incrementAndGet();
            executor.submit(runnable);
        }

        try {
            downLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        callback.progress(1.0f);
    }

}
