package com.ivyft.katta.lucene;

import com.ivyft.katta.lib.lucene.*;
import com.ivyft.katta.util.NodeConfiguration;
import org.apache.solr.client.solrj.SolrQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URI;

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
public class LuceneServerTest {

    LuceneServer server = null;

    @Before
    public void setUp() throws Exception {
        SolrHandler.init(new File("/Volumes/Study/IntelliJ/yiidata/katta1/data/solr"));
        server = new LuceneServer();
        server.init("hello", new NodeConfiguration());

        String indexPath = "/Volumes/Study/IntelliJ/yiidata/katta1/data/katta-shards/zhenqin-pro102_20000/userindex#OV92iJRxjPio5PX18JR";
        System.out.println(new File(indexPath).toURI());
        server.addShard("word",
                 new File(indexPath).toURI(),
                "userindex");
    }


    @After
    public void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    public void testGroup() throws Exception {
        SolrQuery query = new SolrQuery("USER_FOLLOWINGS:0");
        query.setStart(60);
        query.setRows(20);

        query.setFacet(true);
        query.addFacetField("CNT_FOLLOWINGS");

        System.out.println(server.getShards());

        GroupResultWritable group = server.group(new QueryWritable(query),
                new String[]{"word"},
                5000);
        System.out.println(group.getTotalHitCount());
        System.out.println(group.get().size());

        FacetResultWritable facet = server.facet(new QueryWritable(query),
                new String[]{"word"},
                5000);
        System.out.println(facet.getTotalHitCount());
        System.out.println(facet.get().size());
    }
}
