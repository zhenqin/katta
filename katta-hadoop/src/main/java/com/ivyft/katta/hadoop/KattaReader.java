package com.ivyft.katta.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;

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
     * Katta export socket port
     */
    protected int port = 5880;


    /**
     * Hadoop Reader 返回的keyField
     */
    protected String keyField;


    /**
     * Reader一次性取出的数量
     */
    protected int limit = 200;


    /**
     * Solr Query
     */
    protected SolrQuery query;


    /**
     * 导出查询的Index Name
     */
    protected String[] indexNames;


    /**
     * 多线程的线程池，建议大小为最少Shard的数量
     */
    protected ExecutorService executor;


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
                                          ExecutorService executor) throws Exception {
        return new KattaReader(indexNames, query, executor);
    }


    protected KattaReader(String[] indexNames, SolrQuery query, ExecutorService executor) {
        this.indexNames = indexNames;
        this.query = query;
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
        Configuration configuration = new Configuration(false);
        KattaInputFormat.setInputKey(configuration, keyField);
        KattaInputFormat.setLimit(configuration, limit);
        KattaInputFormat.setSocketPort(configuration, port);
        KattaInputFormat.setInputQuery(configuration, query);
        KattaInputFormat.setIndexNames(configuration, indexNames);

        List<KattaInputSplit> splits = KattaSpliter.calculateSplits(configuration);

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


    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getKeyField() {
        return keyField;
    }

    public void setKeyField(String keyField) {
        this.keyField = keyField;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public SolrQuery getQuery() {
        return query;
    }

    public void setQuery(SolrQuery query) {
        this.query = query;
    }
}
