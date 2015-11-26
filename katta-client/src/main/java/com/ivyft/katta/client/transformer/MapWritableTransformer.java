package com.ivyft.katta.client.transformer;

import com.ivyft.katta.client.ResultTransformer;
import org.apache.hadoop.io.MapWritable;

import java.util.Collection;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-12-19
 * Time: 下午6:19
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class MapWritableTransformer implements ResultTransformer<MapWritable> {


    private MapWritable map = new MapWritable();


    public MapWritableTransformer() {

    }

    @Override
    public void transform(Object obj, Collection<String> shards) {
        if(obj instanceof MapWritable) {
            map.putAll((MapWritable)obj);
        }
    }

    @Override
    public MapWritable getResult() {
        return map;
    }
}
