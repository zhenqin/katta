package com.ivyft.katta.client;

import java.util.Collection;

/**
 *
 * 该类为 Katta 结果归集类的总接口类。
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-12-19
 * Time: 下午4:50
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public interface ResultTransformer<T> {


    /**
     * 分片数据合并，如果是搜索，还需要排序
     * @param obj
     * @param shards
     */
    public void transform(Object obj, Collection<String> shards);


    /**
     * 获取结果
     * @return
     */
    public T getResult();
}
