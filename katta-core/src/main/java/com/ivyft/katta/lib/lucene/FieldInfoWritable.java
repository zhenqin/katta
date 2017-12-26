package com.ivyft.katta.lib.lucene;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 2017/12/21
 * Time: 18:54
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class FieldInfoWritable implements Writable {


    String shard;


    final List<List<Object>> result = new ArrayList<>();

    public FieldInfoWritable() {

    }

    public FieldInfoWritable(String shard, List<List<Object>> result) {
        this.shard = shard;
        this.result.addAll(result);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(shard);

        out.writeInt(result.size());
        for (List<Object> list : result) {
            for (Object o : list) {
                out.writeUTF((o instanceof String) ? (String)o : String.valueOf(o));
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        shard = in.readUTF();

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            List<Object> line = new ArrayList<>();
            int fieldSize = 6;
            for (int j = 0; j < fieldSize; j++) {
                line.add(in.readUTF());
            }
            this.result.add(line);
        }
    }


    public String getShard() {
        return shard;
    }

    public List<List<Object>> getResult() {
        return result;
    }


    @Override
    public String toString() {
        return "FieldInfoWritable{" +
                "shard='" + shard + '\'' +
                ", result=" + result +
                '}';
    }
}
