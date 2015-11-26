package com.ivyft.katta.client.transformer;

import com.ivyft.katta.client.ResultTransformer;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-12-19
 * Time: 下午6:07
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class TotalTransformer implements ResultTransformer<Integer> {


    protected final AtomicInteger count = new AtomicInteger(0);


    public TotalTransformer() {

    }

    @Override
    public void transform(Object obj, Collection<String> shards) {
        if(obj instanceof Number) {
            count.addAndGet(((Number)obj).intValue());
        }

    }

    @Override
    public Integer getResult() {
        return count.get();
    }
}
