package com.ivyft.katta.client;

import java.util.Collection;

/**
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


    public void transform(Object obj, Collection<String> shards);


    public T getResult();
}
