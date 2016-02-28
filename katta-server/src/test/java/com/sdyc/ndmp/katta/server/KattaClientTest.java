package com.sdyc.ndmp.katta.server;

import com.ivyft.katta.lib.lucene.ILuceneServer;
import com.ivyft.katta.lib.lucene.QueryWritable;
import com.ivyft.katta.server.protocol.KattaServerProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.solr.client.solrj.SolrQuery;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/2/26
 * Time: 18:35
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaClientTest {


    Configuration conf = new Configuration();


    KattaServerProtocol kattaServerProtocol;

    @Before
    public void setUp() throws Exception {
        System.out.println("===========================");
        long start = System.currentTimeMillis();
        kattaServerProtocol = RPC.getProtocolProxy(KattaServerProtocol.class,
                ILuceneServer.versionID, new InetSocketAddress("localhost", 20020), conf).getProxy();
        System.out.println("connect cost: " + (System.currentTimeMillis() - start));
    }


    @Test
    public void testHadoopRpc() throws Exception {
        kattaServerProtocol.addShard("userindex#2PD95Ggl2tWnSynu8gX", "userindex", "hdfs:/user/katta/userindex/2PD95Ggl2tWnSynu8gX");

    }

    @Test
    public void testCount() throws Exception {
        SolrQuery solrQuery = new SolrQuery("*:*");
        QueryWritable q = new QueryWritable(solrQuery);
        for (int i = 0; i < 20; i++) {
            long start = System.currentTimeMillis();
            int count = kattaServerProtocol.count(q, new String[]{"userindex#2PD95Ggl2tWnSynu8gX"}, 60000);
            System.out.println("cost: " + (System.currentTimeMillis() - start)
                    + " ms  count:   " + count);

        }
    }
}
