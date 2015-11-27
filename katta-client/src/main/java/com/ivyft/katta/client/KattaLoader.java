package com.ivyft.katta.client;

import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/27
 * Time: 19:26
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public interface KattaLoader<T> {


    public int add(Pair<T> pair);


    public int addBean(String shardId, T message);


    public int addBeans(List<Pair<T>> list);


    public void commit();


    public void rollback();


}


class Pair<T> {
    protected final String shardId;
    protected final T bean;


    public Pair(T bean, String shardId) {
        this.bean = bean;
        this.shardId = shardId;
    }

    public String getShardId() {
        return shardId;
    }

    public T getBean() {
        return bean;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair<?> pair = (Pair<?>) o;

        return shardId.equals(pair.shardId);

    }

    @Override
    public int hashCode() {
        return shardId.hashCode();
    }
}
