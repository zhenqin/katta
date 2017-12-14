package com.ivyft.katta.node;

import com.ivyft.katta.Katta;
import com.ivyft.katta.client.KattaClient;
import com.ivyft.katta.lib.lucene.Hits;
import com.ivyft.katta.lib.lucene.QueryResponse;
import com.ivyft.katta.util.ZkConfiguration;
import org.apache.solr.client.solrj.SolrQuery;
import org.junit.Before;
import org.junit.Test;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/2
 * Time: 17:35
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaSearchTest {


    protected KattaClient kattaClient;


    @Before
    public void setUp() throws Exception {
        kattaClient = new KattaClient(new ZkConfiguration());
    }

    @Test
    public void testSearch() throws Exception {
        QueryResponse response = kattaClient.query(new SolrQuery("*:*"), new String[]{"userindex"});
        long numFount = response.getNumFount();
        System.out.println(numFount);
    }
}
