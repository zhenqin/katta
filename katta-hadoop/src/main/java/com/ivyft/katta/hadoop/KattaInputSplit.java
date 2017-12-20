package com.ivyft.katta.hadoop;

import com.ivyft.katta.codec.Serializer;
import com.ivyft.katta.codec.jdkserializer.JdkSerializer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.solr.client.solrj.SolrQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * <pre>
 * 
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-9-29
 * Time: 下午3:46
 * To change this template use File | Settings | File Templates.
 * 
 * </pre>
 * 
 * @author ZhenQin
 */
public class KattaInputSplit extends InputSplit implements Writable,
		org.apache.hadoop.mapred.InputSplit {

    /**
     * 分片节点地址
     */
	private String host;


    /**
     * 分片节点端口号
     */
	private int port = 5880;



    /**
     * 需要查询的ShardName
     */
    private String shardName;


    /**
     * 查询端口号
     */
	private SolrQuery query;


    /**
     * input key所在的字段名称
     */
    private String keyField;


	/**
	 * 每次查询的最大量
	 */
	private int limit = 200;

    /**
     * log
     */
	private static final Logger LOG = LoggerFactory.getLogger(KattaInputSplit.class);


    /**
     * default constractor
     */
	public KattaInputSplit() {

	}

	public KattaInputSplit(String host,
                           int port,
                           String shardName,
                           SolrQuery query,
                           String inputKey,
                           int limit) {
		this.host = host;
		this.port = port;
        this.shardName = shardName;
		this.query = query;
		this.keyField = inputKey;
		this.limit = limit;
	}

	@Override
	public long getLength() throws IOException {
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException {
		return new String[]{ host + ":" + port};
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(host);
		out.writeInt(port);
        out.writeUTF(shardName);
		out.writeInt(limit);
		out.writeUTF(keyField);

        Serializer<Serializable> serializer = new JdkSerializer();
        byte[] bytes = serializer.serialize(query);
		out.writeInt(bytes.length);
        out.write(bytes);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		host = in.readUTF();
		port = in.readInt();
        shardName = in.readUTF();
		limit = in.readInt();
		keyField = in.readUTF();

        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        Serializer<Serializable> serializer = new JdkSerializer();
        query = (SolrQuery) serializer.deserialize(bytes);
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}


    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    public String getShardName() {
        return shardName;
    }

    public SolrQuery getQuery() {
        return query;
    }

    public void setQuery(SolrQuery query) {
        this.query = query;
    }


    public String getKeyField() {
        return keyField;
    }

    public void setKeyField(String keyField) {
        this.keyField = keyField;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }


    @Override
    public String toString() {
        return "KattaInputSplit{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", query=" + query +
                ", keyField='" + keyField + '\'' +
                ", limit=" + limit +
                '}';
    }
}
