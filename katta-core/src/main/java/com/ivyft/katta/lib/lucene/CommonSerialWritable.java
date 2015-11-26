package com.ivyft.katta.lib.lucene;

import com.ivyft.katta.serializer.jdkserializer.JdkSerializer;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 14-1-14
 * Time: 下午3:29
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public class CommonSerialWritable<T extends Serializable> implements Writable {



    private T value;



    public CommonSerialWritable() {

    }


    public CommonSerialWritable(T value) {
        if(value == null) {
            throw new NullPointerException("value must not be null.");
        }
        this.value = value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        JdkSerializer<T> jdkSerializer = new JdkSerializer<T>();

        byte[] bytes = jdkSerializer.serialize(value);
        int length = bytes.length;
        out.writeInt(length);
        out.write(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        JdkSerializer<T> jdkSerializer = new JdkSerializer<T>();
        int length = in.readInt();

        byte[] bytes = new byte[length];
        in.readFully(bytes);
        this.value = jdkSerializer.deserialize(bytes);
    }


    public T getValue() {
        return value;
    }
}
