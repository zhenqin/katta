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
                                          ExecutorService executor) throws Exception {
        /*
        KattaInputFormat.setInputKey(conf, keyField);
        KattaInputFormat.setLimit(configuration, limit);
        KattaInputFormat.setSocketPort(configuration, port);
        */

        String inputKey = KattaInputFormat.getInputKey(conf);
        if(StringUtils.isBlank(inputKey)) {
            throw new IllegalArgumentException("input key must not be blank.");
        }


        KattaInputFormat.setInputQuery(conf, query);
        KattaInputFormat.setIndexNames(conf, indexNames);
        List<KattaInputSplit> splits = KattaSpliter.calculateSplits(conf);
        return new KattaReader(indexNames, query, splits, executor);
    }





    /**
     * Solr Cloud Reader
     * @param indexNames
     * @param query
     * @param executor
     * @return
     * @throws Exception
     */
    public static KattaReader getSingleKattaInstance(String[] indexNames,
                                                     SolrQuery query,
                                                     String host,
                                                     int port,
                                                     String inputKey,
                                                     int limit,
                                                     ExecutorService executor) throws Exception {
        List<KattaInputSplit> splits = new ArrayList<KattaInputSplit>(indexNames.length);
        for (String name : indexNames) {
            splits.add(new KattaInputSplit(host, port, name, query, inputKey, limit));
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
