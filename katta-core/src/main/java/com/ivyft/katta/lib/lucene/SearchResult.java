package com.ivyft.katta.lib.lucene;

import org.apache.lucene.search.ScoreDoc;

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
public class SearchResult {

    /**
     * 命中的数量
     */
    protected final int totalHits;


    /**
     * 文档的ID
     */
    protected final ScoreDoc[] scoreDocs;

    /**
     * 查询耗时
     */
    protected final long qtime;


    /**
     * 索引的排序
     */
    private int searchCallIndex;


    /**
     * 构造方法
     * @param totalHits 命中的数量
     * @param scoreDocs 文档的ID
     * @param searchCallIndex 索引的排序
     * @param qtime 查询耗时
     */
    public SearchResult(int totalHits, ScoreDoc[] scoreDocs, int searchCallIndex, long qtime) {
        this.totalHits = totalHits;
        this.scoreDocs = scoreDocs;
        this.searchCallIndex = searchCallIndex;
        this.qtime = qtime;
    }

    public int getTotalHits() {
        return totalHits;
    }

    public ScoreDoc[] getScoreDocs() {
        return scoreDocs;
    }

    public long getQtime() {
        return qtime;
    }

    public int getSearchCallIndex() {
        return searchCallIndex;
    }
}
