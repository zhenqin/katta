package com.ivyft.katta.client.transformer;

import com.ivyft.katta.client.ResultTransformer;
import com.ivyft.katta.lib.lucene.GroupResultWritable;
import org.apache.hadoop.io.Writable;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 14-1-9
 * Time: 下午4:34
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class GroupResultWritableTransformer<T extends Writable> implements ResultTransformer<Set<T>> {


    protected Set<T> group = new HashSet<T>();

    public GroupResultWritableTransformer() {

    }

    @Override
    public void transform(Object obj, Collection<String> shards) {
        if(obj == null) {
            return;
        }
        if(obj instanceof GroupResultWritable) {
            GroupResultWritable<T> groupResult = (GroupResultWritable<T>)obj;
            group.addAll(groupResult.get());
        }
    }

    @Override
    public Set<T> getResult() {
        return group;
    }
}
