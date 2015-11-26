package com.ivyft.katta.lib.lucene;

import com.ivyft.katta.codec.jdkserializer.JdkSerializer;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-12-19
 * Time: 下午1:07
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class ResponseWritable implements Writable {


    private QueryResponse response;


    public ResponseWritable() {
    }

    public ResponseWritable(QueryResponse response) {
        this.response = response;
    }

    /**
     * Serialize the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        JdkSerializer jdkSerializer = new JdkSerializer<QueryResponse>();

        byte[] bytes = jdkSerializer.serialize(response);
        int length = bytes.length;
        out.writeInt(length);
        out.write(bytes);
    }

    /**
     * Deserialize the fields of this object from <code>in</code>.
     * <p/>
     * <p>For efficiency, implementations should attempt to re-use storage in the
     * existing object where possible.</p>
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        JdkSerializer jdkSerializer = new JdkSerializer<QueryResponse>();
        int length = in.readInt();

        byte[] bytes = new byte[length];
        in.readFully(bytes);
        this.response = (QueryResponse) jdkSerializer.deserialize(bytes);
    }

    public QueryResponse getResponse() {
        return response;
    }
}
