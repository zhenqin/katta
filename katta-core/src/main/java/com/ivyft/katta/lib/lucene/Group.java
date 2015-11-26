package com.ivyft.katta.lib.lucene;

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
 * Time: 下午5:11
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class Group<T extends Writable> {


    /**
     * Group的结果来自于那个shard
     */
    protected String shardName;

    /**
     * 查询耗时
     */
    protected long qtime;


    /**
     * Group结果保存的Set， 该Set中不能存在Null的数据
     */
    protected Set<T> groupResult = new HashSet<T>();



    /**
     * 命中稳定的最大评分
     */
    protected float maxScore;


    /**
     * 命中的文档数量
     */
    protected int totalHitCount;

    /**
     * 构造方法
     * @param shardName Group的结果来自于那个shard
     * @param qtime 查询耗时
     */
    public Group(String shardName, long qtime) {
        this.shardName = shardName;
        this.qtime = qtime;
    }

    public int size() {
        return groupResult.size();
    }

    public boolean addAll(Collection<T> c) {
        return groupResult.addAll(c);
    }

    public boolean add(T o) {
        if(o != null) {
            return groupResult.add(o);
        }
        return false;
    }

    public void clear() {
        groupResult.clear();
    }

    public boolean isEmpty() {
        return groupResult.isEmpty();
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
}
