package com.ivyft.katta.client.transformer;

import com.ivyft.katta.client.ResultTransformer;
import com.ivyft.katta.lib.lucene.ArrayMapWritable;
import org.apache.hadoop.io.MapWritable;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 14-1-9
 * Time: 下午4:00
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class ArrayMapWritableTransformer  implements ResultTransformer<List<MapWritable>> {


    protected final LinkedList<MapWritable> list = new LinkedList<MapWritable>();

    public ArrayMapWritableTransformer() {

    }


    @Override
    public void transform(Object obj, Collection<String> shards) {
        if(obj == null) {
            return;
        }
        if(obj instanceof ArrayMapWritable) {
            list.addAll(((ArrayMapWritable)obj).get());
        }
    }

    @Override
    public List<MapWritable> getResult() {
        return list;
    }
}
