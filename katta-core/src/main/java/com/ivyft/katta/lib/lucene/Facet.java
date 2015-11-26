package com.ivyft.katta.lib.lucene;

import org.apache.hadoop.io.Writable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 14-1-9
 * Time: 下午5:11
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class Facet<T extends Writable> {


    /**
     * Facet的结果来自哪个Shard？
     */
    protected String shardName;

    /**
     * 查询耗时
     */
    protected long qtime;


    /**
     * 命中稳定的最大评分
     */
    protected float maxScore;


    /**
     * 命中的文档数量
     */
    protected int totalHitCount;


    /**
     * Group命中文档的最大数量
     */
    protected int totalGroupedHitCount;


    /**
     * Group的组数量
     */
    protected int totalGroupCount;


    /**
     * Facet后负载的数据
     */
    protected Map<T, AtomicInteger> facetResult = new ConcurrentHashMap<T, AtomicInteger>();


    /**
     * 构造方法
     * @param shardName Facet Result Shard Name
     * @param qtime 消耗时间
     */
    public Facet(String shardName, long qtime) {
        this.shardName = shardName;
        this.qtime = qtime;
    }

    public int size() {
        return facetResult.size();
    }

    public boolean isEmpty() {
        return facetResult.isEmpty();
    }

    public AtomicInteger get(Object key) {
        return facetResult.get(key);
    }


    public AtomicInteger add(T key, Integer value) {
        if(key != null) {
            return facetResult.put(key, new AtomicInteger(value));
        }
        return new AtomicInteger(-1);
    }


    public AtomicInteger put(T key, AtomicInteger value) {
        return facetResult.put(key, value);
    }

    public void putAll(Map<? extends T, ? extends AtomicInteger> m) {
        facetResult.putAll(m);
    }

    public void clear() {
        facetResult.clear();
    }


    public String getShardName() {
        return shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    public long getQtime() {
        return qtime;
    }

    public void setQtime(long qtime) {
        this.qtime = qtime;
    }

    public float getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(float maxScore) {
        this.maxScore = maxScore;
    }

    public int getTotalHitCount() {
        return totalHitCount;
    }

    public void setTotalHitCount(int totalHitCount) {
        this.totalHitCount = totalHitCount;
    }

    public int getTotalGroupedHitCount() {
        return totalGroupedHitCount;
    }

    public void setTotalGroupedHitCount(int totalGroupedHitCount) {
        this.totalGroupedHitCount = totalGroupedHitCount;
    }

    public int getTotalGroupCount() {
        return totalGroupCount;
    }

    public void setTotalGroupCount(Integer totalGroupCount) {
        if(totalGroupCount == null) {
            this.totalGroupCount = 0;
            return;
        }
        this.totalGroupCount = totalGroupCount;
    }

    public Map<T, AtomicInteger> getFacetResult() {
        return facetResult;
    }
}
