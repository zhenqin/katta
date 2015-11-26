package com.ivyft.katta.lib.lucene;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-12-19
 * Time: 下午1:07
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class QueryResponse implements Serializable {


    /**
     * 序列化
     */
    private final static long serialVersionUID = 1L;


    /**
     * 查询耗时
     */
    private long qTime = 0;


    /**
     * 查询到的总数量
     */
    private long numFount = 0;


    /**
     */
    private float maxScore = 0.0f;


    /**
     * 负载的数据
     */
    private List<? extends Map<String, ? extends Serializable>> docs;


    /**
     * 构造方法
     */
    public QueryResponse() {

    }

    public long getQTime() {
        return qTime;
    }

    public void setQTime(long qTime) {
        this.qTime = Math.max(this.qTime, qTime);
    }

    public long getNumFount() {
        return numFount;
    }

    void setNumFount(long numFount) {
        this.numFount = numFount;
    }


    public void addNumFount(long numFount) {
        this.numFount += numFount;
    }

    public float getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(float maxScore) {
        this.maxScore = Math.max(this.maxScore, maxScore);
    }

    public <T> List<T> getDocs() {
        return (List<T>) docs;
    }

    void setDocs(List<? extends Map<String, ? extends Serializable>> docs) {
        this.docs = docs;
    }


    public void addDocs(List docs) {
        if(this.docs == null) {
            setDocs(docs);
        } else {
            this.docs.addAll(docs);
        }
    }
}
