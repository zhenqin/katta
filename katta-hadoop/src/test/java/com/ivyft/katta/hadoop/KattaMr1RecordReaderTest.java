package com.ivyft.katta.hadoop;

import com.ivyft.katta.hive.KattaMr1RecordReader;
import com.ivyft.katta.hive.SolrDocumentSerde;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.solr.client.solrj.SolrQuery;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 2017/12/26
 * Time: 09:09
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaMr1RecordReaderTest {


    Configuration conf = new Configuration();

    List<KattaInputSplit> splits = null;


    @Before
    public void setUp() throws Exception {
        // USER_ID:1707462091   USER_FOLLOWINGS:0
        SolrQuery query = new SolrQuery("USER_ID:1707462091");
        query.setStart(60);
        query.setRows(20);

        KattaInputFormat.setZookeeperServers(conf, "localhost:2181");
        KattaInputFormat.setInputKey(conf, "USER_URN");
        KattaInputFormat.setLimit(conf, 10);
        KattaInputFormat.setInputQuery(conf, query);
        KattaInputFormat.setIndexNames(conf, "userindex");
        splits = KattaSpliter.calculateSplits(conf);
    }


    @Test
    public void testRead() throws Exception {
        KattaMr1RecordReader reader = new KattaMr1RecordReader(splits.get(1), conf);
        Text key = reader.createKey();
        MapWritable value = reader.createValue();
        while (reader.next(key, value)) {
            System.out.println("==============================");
            for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
                System.out.println(entry.getKey() + "    " + entry.getValue());
            }
        }
    }


    @Test
    public void testSerde() throws Exception {
        Properties prop = new Properties();
        prop.setProperty("zookeeper.servers", "localhost:2181");
        prop.setProperty("katta.hadoop.indexes", "userindex");
        prop.setProperty("katta.input.query", "*:*");
        prop.setProperty(serdeConstants.LIST_COLUMNS, "USER_URN,USER_ID,USER_FOLLOWINGS,CNT_FOLLOWINGS");
        prop.setProperty(serdeConstants.STRING_TYPE_NAME, "STRING,STRING,STRING,STRING");

        SolrDocumentSerde serde = new SolrDocumentSerde();
        serde.initialize(conf, prop);

        KattaMr1RecordReader reader = new KattaMr1RecordReader(splits.get(0), conf);
        Text key = reader.createKey();
        MapWritable value = reader.createValue();
        while (reader.next(key, value)) {
            Object values = serde.deserialize(value);
            System.out.println(values);
        }
    }
}
