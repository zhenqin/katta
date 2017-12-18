package com.ivyft.katta.node;

import com.ivyft.katta.client.IndexOperator;
import com.ivyft.katta.client.KattaParams;
import com.ivyft.katta.lib.lucene.QueryResponse;
import com.ivyft.katta.util.ZkConfiguration;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

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


    protected IndexOperator indexOperator;


    @Before
    public void setUp() throws Exception {
        indexOperator = new IndexOperator(new ZkConfiguration(), "userindex");
    }

    @After
    public void tearDown() throws Exception {
        indexOperator.close();
    }

    @Test
    public void testSearch() throws Exception {
        SolrQuery query = new SolrQuery("CNT_FOLLOWINGS:(2 OR 3) AND USER_FOLLOWINGS:0");
        query.setStart(60);
        query.setRows(20);

        query.addSort("USER_FOLLOWINGS", SolrQuery.ORDER.asc);
        query.add(KattaParams.KATTA_SORT_FIELD_TYPE, "USER_FOLLOWINGS:" + KattaParams.Type.STRING);

        query.addSort("USER_ID", SolrQuery.ORDER.asc);
        query.add(KattaParams.KATTA_SORT_FIELD_TYPE, "USER_ID:" + KattaParams.Type.STRING);

        QueryResponse response = indexOperator.query(query);
        System.out.println("rpc remote cose time: " + response.getQTime() + " ms");

        long numFount = response.getNumFount();
        Collection<SolrDocument> docs = response.getDocs();
        System.out.println(numFount);
        System.out.println(docs.size());

        //for (SolrDocument doc : docs) {
        //    System.out.println(doc);
        //}

        for (int i = 0; i < 10; i++) {
            response = indexOperator.query(query);
            System.out.println("rpc remote cose time: " + response.getQTime() + " ms");

            numFount = response.getNumFount();
            System.out.println(numFount);
            System.out.println(docs.size());
        }
    }


    @Test
    public void testFact() throws Exception {
        SolrQuery query = new SolrQuery("USER_FOLLOWINGS:0");
        query.setStart(60);
        query.setRows(20);

        query.setFacet(true);
        query.addFacetField("CNT_FOLLOWINGS");
        Set group = indexOperator.group(query);
        System.out.println(group);

        Map<Object, Integer> facet = indexOperator.facet(query);
        System.out.println(facet);

    }
}
