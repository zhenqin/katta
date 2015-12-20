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



    protected final String indexName;



    private final String commitId;


    protected final Set<ShardRange> commits;


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
