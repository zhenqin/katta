package com.sdyc.ndmp.katta.server;

import com.ivyft.katta.hadoop.KattaReader;
import com.ivyft.katta.lib.lucene.*;
import com.ivyft.katta.node.ShardManager;
import com.ivyft.katta.server.KattaBooter;
import com.ivyft.katta.server.lucene.KattaLuceneServer;
import com.ivyft.katta.util.NodeConfiguration;
import com.ivyft.katta.util.ThrottledInputStream;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 17/12/18
 * Time: 14:55
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaLuceneServerTest {

    KattaLuceneServer server = null;

    private static Logger LOG = LoggerFactory.getLogger("test");



    @Before
    public void setUp() throws Exception {
        SolrHandler.init(new File("/Volumes/Study/IntelliJ/yiidata/katta1/data/solr"));
        server = new KattaLuceneServer();

        NodeConfiguration _nodeConf = new NodeConfiguration();

        //初始化ShardManager，ShardManager是复制文件的管理
        final ShardManager shardManager;

        String _nodeName = "zhenqin-pro102:20000";
        File shardsFolder = new File(_nodeConf.getShardFolder(), _nodeName.replaceAll(":", "_"));
        LOG.info("local shard folder: " + shardsFolder.getAbsolutePath());


        shardManager = new ShardManager(_nodeConf, shardsFolder, server);

        server.setShardManager(shardManager);

        server.init("hello", _nodeConf);

    }


    @After
    public void tearDown() throws Exception {
        server.shutdown();
    }


    @Test
    public void testQuery() throws Exception {
        String shard = "userindex#OV92iJRxjPio5PX18JR";
        SolrQuery query = new SolrQuery("USER_FOLLOWINGS:0");
        query.setStart(60);
        query.setRows(20);

        ResponseWritable responseWritable = server.query(new QueryWritable(query), new String[]{shard}, 5000);
        QueryResponse response = responseWritable.getResponse();
        System.out.println(response.getNumFount());

        Collection<SolrDocument> docs = response.<SolrDocument>getDocs();
        for (SolrDocument doc : docs) {
            System.out.println(doc.get("USER_FOLLOWINGS").getClass().getName());
        }
    }



    @Test
    public void testKattaReader() throws Exception {
        String shard = "userindex#OV92iJRxjPio5PX18JR";
        SolrQuery query = new SolrQuery("USER_FOLLOWINGS:0");
        query.setStart(60);
        query.setRows(20);

        KattaReader reader = KattaReader.getSingleKattaInstance(new String[]{shard},
                query, "localhost", 5881,
                "USER_ID", 10,
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
                System.out.println("======================>"  + process);
            }
        });

        System.out.println(counter.get());
    }

    @Test
    public void testGroup() throws Exception {
        String shard = "userindex#OV92iJRxjPio5PX18JR";
        SolrQuery query = new SolrQuery("USER_FOLLOWINGS:0");
        query.setStart(60);
        query.setRows(20);

        query.setFacet(true);
        query.addFacetField("CNT_FOLLOWINGS");

        System.out.println(server.getShards());

        GroupResultWritable group = server.group(new QueryWritable(query),
                new String[]{shard},
                5000);
        System.out.println(group.getTotalHitCount());
        System.out.println(group.get().size());

        FacetResultWritable facet = server.facet(new QueryWritable(query),
                new String[]{shard},
                5000);
        System.out.println(facet.getTotalHitCount());
        System.out.println(facet.get().size());
    }
}
