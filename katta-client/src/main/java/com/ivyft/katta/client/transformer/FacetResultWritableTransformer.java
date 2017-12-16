package com.ivyft.katta.client.transformer;

import com.ivyft.katta.client.ResultTransformer;
import com.ivyft.katta.lib.lucene.FacetResultWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 14-1-9
 * Time: 下午4:33
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class FacetResultWritableTransformer<T extends Writable> implements ResultTransformer<Map<T, Integer>> {


    /**
     * Facet Result
     */
    protected Map<T, AtomicInteger> facet = new HashMap<T, AtomicInteger>();



    /**
     * 命中稳定的最大评分
     */
    protected float maxScore = 0.0f;


    /**
     * 命中的文档数量
     */
    protected AtomicInteger totalHitCount = new AtomicInteger(0);


    /**
     * Group命中文档的最大数量
     */
    protected AtomicInteger totalGroupedHitCount = new AtomicInteger(0);



    public FacetResultWritableTransformer() {

    }

    @Override
    public void transform(Object obj, Collection<String> shards) {
        if(obj == null) {
            return;
        }
        if(obj instanceof FacetResultWritable) {
            FacetResultWritable<T> facetResult = (FacetResultWritable<T>)obj;
            setMaxScore(facetResult.getMaxScore());
            addTotalHitCount(facetResult.getTotalHitCount());
            addTotalGroupedHitCount(facetResult.getTotalGroupedHitCount());

            Map<T, IntWritable> m = facetResult.get();
            for (Map.Entry<T, IntWritable> entry : m.entrySet()) {
                AtomicInteger intCount = facet.get(entry.getKey());
                int add = entry.getValue() == null ? 0 : entry.getValue().get();
                if(intCount == null) {
                    intCount = new AtomicInteger(add);

                    facet.put(entry.getKey(), intCount);
                } else {
                    intCount.addAndGet(add);
                }
            }
        }
    }

    @Override
    public Map<T, Integer> getResult() {
        Map<T, Integer> result = new HashMap<T, Integer>(facet.size());
        for (Map.Entry<T, AtomicInteger> entry : facet.entrySet()) {
            result.put(entry.getKey(), entry.getValue().intValue());
        }
        return result;
    }



    public float getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(float maxScore) {
        this.maxScore = Math.max(this.maxScore, maxScore);
    }

    public int getTotalHitCount() {
        return totalHitCount.get();
    }

    public void addTotalHitCount(int totalHitCount) {
        this.totalHitCount.addAndGet(totalHitCount);
    }

    public int getTotalGroupedHitCount() {
        return totalGroupedHitCount.get();
    }

    public void addTotalGroupedHitCount(int totalGroupedHitCount) {
        this.totalGroupedHitCount.addAndGet(totalGroupedHitCount);
    }

    public int getTotalGroupCount() {
        return facet.size();
    }
}
