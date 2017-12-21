package com.ivyft.katta.node;

import com.ivyft.katta.hadoop.KattaReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 2017/12/21
 * Time: 16:28
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaReaderTest {

    @Test
    public void testKattaReader() throws Exception {
        SolrQuery query = new SolrQuery("*:*");
        query.setStart(60);
        query.setRows(20);

        KattaReader reader = KattaReader.getInstance(
                new String[]{"userindex"},
                query,
                new Configuration(),
                "localhost:2181",
                "USER_ID",
                10000,
                Executors.newScheduledThreadPool(5));

        final AtomicInteger counter = new AtomicInteger(0);
        reader.processKatta(new KattaReader.DocumentCallback() {
            @Override
            public void callback(SolrDocument document) {
                counter.incrementAndGet();
            }

            @Override
            public void progress(float process) {
                System.out.println("----------------------->"  + process);
            }
        });

        System.out.println(counter.get());
    }



    @Test
    public void testSingleKattaReader() throws Exception {
        String shard = "userindex#OV92iJRxjPio5PX18JR";
        SolrQuery query = new SolrQuery("USER_FOLLOWINGS:0");
        query.setStart(60);
        query.setRows(20);

        KattaReader reader = KattaReader.getSingleKattaInstance(new String[]{shard},
                query, "localhost", 5880,
                "USER_ID", 100, 16, 52,
                Executors.newScheduledThreadPool(5));

        final AtomicInteger counter = new AtomicInteger(0);
        reader.processKatta(new KattaReader.DocumentCallback() {
            @Override
            public void callback(SolrDocument document) {
                counter.incrementAndGet();
                System.out.println(document);
            }

            @Override
            public void progress(float process) {
                System.out.println("----------------------->"  + process);
            }
        });

        System.out.println(counter.get());
    }

}
