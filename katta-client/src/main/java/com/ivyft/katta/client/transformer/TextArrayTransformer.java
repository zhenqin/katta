package com.ivyft.katta.client.transformer;

import com.ivyft.katta.client.ResultTransformer;
import com.ivyft.katta.client.mapfile.TextArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-12-20
 * Time: 下午3:13
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public class TextArrayTransformer implements ResultTransformer<List<String>> {


    protected final List<String> stringResults = new LinkedList<String>();

    public TextArrayTransformer() {

    }

    @Override
    public void transform(Object obj, Collection<String> shards) {
        if(obj instanceof TextArrayWritable) {
            TextArrayWritable taw = (TextArrayWritable)obj;
            for (Writable w : taw.array.get()) {
                Text text = (Text) w;
                stringResults.add(text.toString());
            }
        }
    }

    @Override
    public List<String> getResult() {
        return stringResults;
    }
}
