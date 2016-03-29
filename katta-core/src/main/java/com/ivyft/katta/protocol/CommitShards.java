package com.ivyft.katta.protocol;

import com.ivyft.katta.lib.writer.ShardRange;

import java.io.Serializable;
import java.util.Set;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/20
 * Time: 19:49
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class CommitShards implements Serializable, Cloneable {


    /**
     * 序列化
     */
    private final static long serialVersionUID = 0L;


    /**
     * 索引名称
     */
    protected final String indexName;


    /**
     * Commit
     */
    private final String commitId;


    /**
     * ShardRange
     */
    protected final Set<ShardRange> commits;


    /**
     * 构造方法
     * @param indexName Index Name
     * @param commitId Commit UUID
     * @param commits 一次性 Commit 记录
     */
    public CommitShards(String indexName, String commitId, Set<ShardRange> commits) {
        this.commitId = commitId;
        this.commits = commits;
        this.indexName = indexName;
    }


    public String getCommitId() {
        return commitId;
    }

    public Set<ShardRange> getCommits() {
        return commits;
    }

    public String getIndexName() {
        return indexName;
    }
}
