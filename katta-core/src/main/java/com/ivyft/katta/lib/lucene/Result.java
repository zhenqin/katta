package com.ivyft.katta.lib.lucene;

import java.io.Serializable;
import java.util.List;

/**
 *
 * <p>
 *     搜索结果保存的Field
 * </p>
 *
 *
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-19
 * Time: 上午9:16
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class Result {

    /**
     * 命中的数量
     */
    protected int totalHits;


    /**
     * 文档的ID
     */
    protected List<? extends Serializable> documents;


    /**
     * 查询耗时
     */
    protected long qtime;



    protected float maxScore = 0.0F;

    /**
     * 构造方法
     * @param totalHits 命中的数量
     * @param docs 文档的ID
     * @param qtime 查询耗时
     */
    public Result(int totalHits, List docs, long qtime, float maxScore) {
        this.totalHits = totalHits;
        this.documents = docs;
        this.qtime = qtime;
        this.maxScore = maxScore;
    }

    public int getTotalHits() {
        return totalHits;
    }


    public List<? extends Serializable> getDocuments() {
        return documents;
    }

    public long getQtime() {
        return qtime;
    }


    public float getMaxScore() {
        return maxScore;
    }
}
