package com.ivyft.katta.node;

import com.ivyft.katta.client.KattaClient;
import com.ivyft.katta.client.KattaParams;
import com.ivyft.katta.lib.lucene.QueryResponse;
import com.ivyft.katta.util.ZkConfiguration;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

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
        SolrQuery query = new SolrQuery("CNT_FOLLOWINGS:2 AND USER_FOLLOWINGS:0");
        query.setStart(60);
        query.setRows(20);

        query.addSort("USER_FOLLOWINGS", SolrQuery.ORDER.asc);
        query.add(KattaParams.KATTA_SORT_FIELD_TYPE, "USER_FOLLOWINGS:" + KattaParams.Type.STRING);

        query.addSort("USER_ID", SolrQuery.ORDER.asc);
        query.add(KattaParams.KATTA_SORT_FIELD_TYPE, "USER_ID:" + KattaParams.Type.STRING);

        QueryResponse response = kattaClient.query(query, new String[]{"userindex"});
        long numFount = response.getNumFount();
        Collection<SolrDocument> docs = response.getDocs();
        System.out.println(numFount);
        System.out.println(docs.size());

        for (SolrDocument doc : docs) {
            System.out.println(doc);
        }
    }
}
