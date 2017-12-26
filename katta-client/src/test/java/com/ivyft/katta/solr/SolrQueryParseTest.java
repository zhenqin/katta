package com.ivyft.katta.solr;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.ivyft.katta.lib.lucene.FieldInfoWritable;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.search.Query;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.SolrPluginUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 17/12/15
 * Time: 16:35
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class SolrQueryParseTest {

    SolrCore solrCore = null;


    CoreContainer coreContainer;




    /**
     * 默认的SolrQuery
     */
    public final static SolrQuery DEFAULT_QUERY = new SolrQuery();



    @Before
    public void setUp() throws Exception {
        DEFAULT_QUERY.setStart(0);
        DEFAULT_QUERY.setRows(10);
        DEFAULT_QUERY.set(CommonParams.TIME_ALLOWED, 5 * 1000);


        coreContainer = new CoreContainer("/Volumes/Study/IntelliJ/yiidata/katta1/data/solr");
        coreContainer.load();

        System.out.println(coreContainer.getAllCoreNames());

        solrCore = coreContainer.getCore("userindex");
    }


    @Test
    public void testParse() throws Exception {
        SolrQuery query = new SolrQuery("USER_ID:AFDADFADS OR BBADDA");
        query.addFilterQuery("USER_ID:AFDADFADS OR BBADDA");

        LocalSolrQueryRequest request = new LocalSolrQueryRequest(solrCore, query);

        String q = query.getQuery();
        String[] fq = query.getFilterQueries();

        boolean b1 = StringUtils.isNotBlank(q);
        boolean b2 = ArrayUtils.isNotEmpty(fq);
        //重新定义一个数组, 把q和qs放到一起
        String[] queryStrings = null;
        if(b1 && b2) {
            //重新定义一个数组, 把q和qs放到一起
            queryStrings = new String[fq.length + 1];

            queryStrings[0] = q;
            //这里在复制数组, 一定要小心.
            System.arraycopy(fq, 0, queryStrings, 1, fq.length);
        } else if (b1) {
            queryStrings = new String[]{q};
        } else if(b2) {
            queryStrings = fq;
        } else {
            //q和fq都为null的情况. 直接抛出异常
            throw new IllegalArgumentException("q or fq must not null.");
        }

        List<Query> queries = SolrPluginUtils.parseQueryStrings(request, queryStrings);

        System.out.println(queries);

    }


    @Test
    public void testQuery() throws Exception {
        String indexDir = solrCore.getIndexDir();
        System.out.println(indexDir);


        SolrServer solrServer = new EmbeddedSolrServer(coreContainer, "userindex");
        SolrQuery query = new SolrQuery("*:*");
        QueryResponse response = solrServer.query(query);

        System.out.println(response.getResults().getNumFound());


    }



    @Test
    public void testGetField() throws Exception {
        Map<String, SchemaField> fields = solrCore.getLatestSchema().getFields();

        List<List<Object>> result = new ArrayList<>();
        for (Map.Entry<String, SchemaField> entry : fields.entrySet()) {
            List<Object> list = new ArrayList<>(4);
            SchemaField value = entry.getValue();
            list.add(entry.getKey());
            list.add(value.getType().getTypeName());
            list.add(value.isRequired());
            list.add(value.multiValued());
            list.add(value.indexed());
            list.add(value.stored());

            result.add(list);
        }

        FieldInfoWritable writable = new FieldInfoWritable("userindex", result);
        System.out.println(result.size());

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        writable.write(out);
        byte[] bytes = out.toByteArray();

        FieldInfoWritable w = new FieldInfoWritable();
        w.readFields(ByteStreams.newDataInput(bytes));
        System.out.println(w);
    }
}
