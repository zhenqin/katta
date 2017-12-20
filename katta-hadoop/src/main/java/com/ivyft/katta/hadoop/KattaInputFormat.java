package com.ivyft.katta.hadoop;

import com.google.common.collect.Sets;
import com.ivyft.katta.codec.Serializer;
import com.ivyft.katta.codec.jdkserializer.JdkSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * <pre>
 * solr inputformat
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-9-29
 * Time: 下午3:41
 * To change this template use File | Settings | File Templates.
 * 
 * </pre>
 * 
 * @author ZhenQin
 */
public class KattaInputFormat extends InputFormat<Object, SolrDocument> {


    public static final String ZOOKEEPER_SERVERS = "zookeeper.servers";


    public static final String INPUT_KEY = "katta.input.key";


    public static final String INDEX_NAMES = "katta.hadoop.indexes";


    public static final String INPUT_QUERY = "katta.input.query";

    public static final String INPUT_LIMIT = "katta.input.limit";


    public static final String INCLUDE_FIELDS = "katta.include.fields";



    /**
     * log
     */
    private static Logger log = LoggerFactory.getLogger(KattaInputFormat.class);


    public KattaInputFormat() {

	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		final Configuration hadoopConfiguration = context.getConfiguration();
        return (List)KattaSpliter.calculateSplits(hadoopConfiguration);
	}

	@Override
	public RecordReader<Object, SolrDocument> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		if (!(split instanceof KattaInputSplit))
			throw new IllegalStateException(
					"Creation of a new RecordReader requires a KattaInputSplit instance.");

		final KattaInputSplit sis = (KattaInputSplit) split;

		return new SocketInputReader(sis);
	}



    public static void setIndexNames(Configuration configuration, String... indexes) {
        if(indexes.length <= 0) {
            throw new IllegalArgumentException("indexes must not empty");
        }
        configuration.setStrings(INDEX_NAMES, indexes);
    }


    public static String[] getIndexNames(Configuration configuration) {
        return configuration.getStrings(INDEX_NAMES);
    }

    public static Set<String> getIncludeFields(Configuration conf) {
        return Sets.newHashSet(conf.getStrings(INCLUDE_FIELDS));
    }


    public static void setZookeeperServers(Configuration conf, String zookeeper) {
        conf.setStrings(ZOOKEEPER_SERVERS, zookeeper);
    }

    public static void setIncludeFields(Configuration conf, String[] fields) {
        conf.setStrings(INCLUDE_FIELDS, fields);
    }

    public static void setInputKey(Configuration conf, String fieldName) {
        conf.set(INPUT_KEY, fieldName);
    }

    public static String getInputKey(Configuration conf) {
        return conf.get(INPUT_KEY);
    }

    public static String getInputQueryString(Configuration conf) {
        SolrQuery solrQuery = getInputQuery(conf);
        return solrQuery == null ? null : solrQuery.getQuery();
    }



    public static SolrQuery getInputQuery(Configuration conf) {
        String s = conf.get(INPUT_QUERY);
        if(s != null) {
            Serializer<Serializable> serializer = new JdkSerializer<Serializable>();
            try {
                return (SolrQuery)serializer.deserialize(decodeBytes(s));
            } catch (Exception e) {
                log.info("use solr query " + s);
                return new SolrQuery(s);
            }
        }

        log.info("use solr query *:*");
        return new SolrQuery("*:*");
    }



    /**
     * queryString必须是SolrQuery的序列化字节码构造的。
     * 如果不是，会造成初始化MapReduce错误
     * @param conf
     * @param queryString
     */
    public static void setInputQuery(Configuration conf, String queryString) {
        setInputQuery(conf, new SolrQuery(queryString));
    }


    /**
     * queryString必须是SolrQuery的序列化字节码构造的。
     * 如果不是，会造成初始化MapReduce错误
     * @param conf
     * @param solrQuery
     */
    public static void setInputQuery(Configuration conf, SolrQuery solrQuery) {
        Serializer<Serializable> serializer = new JdkSerializer<Serializable>();
        try {
            conf.set(INPUT_QUERY, encodeBytes(serializer.serialize(solrQuery)));
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static int getLimit(Configuration conf) {
        return conf.getInt(INPUT_LIMIT, 1000);
    }

    public static void setLimit(Configuration conf, int limit) {
        conf.setInt(INPUT_LIMIT, limit);
    }



    public static String encodeBytes(byte[] bytes) {
        StringBuffer strBuf = new StringBuffer();
        for (int i = 0; i < bytes.length; i++) {
            strBuf.append((char) (((bytes[i] >> 4) & 0xF) + ((int) 'a')));
            strBuf.append((char) (((bytes[i]) & 0xF) + ((int) 'a')));
        }

        return strBuf.toString();
    }

    public static byte[] decodeBytes(String str) {
        byte[] bytes = new byte[str.length() / 2];
        for (int i = 0; i < str.length(); i += 2) {
            char c = str.charAt(i);
            bytes[i / 2] = (byte) ((c - 'a') << 4);
            c = str.charAt(i + 1);
            bytes[i / 2] += (c - 'a');
        }
        return bytes;
    }
}
