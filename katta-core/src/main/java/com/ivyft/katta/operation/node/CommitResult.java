package com.ivyft.katta.operation.node;

import java.util.HashMap;
import java.util.Map;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/4/15
 * Time: 10:58
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class CommitResult extends OperationResult {


    /**
     * 序列化
     */
    private static final long serialVersionUID = 1L;


    /**
     * Shard Name, 和异常的 Map
     */
    private Map<String, Exception> exceptionByShard = new HashMap<String, Exception>(3);


    /**
     * Shard Meta 信息
     */
    private Map<String, Map<String, String>> metadataMapByShard = new HashMap<String, Map<String, String>>(3);


    /**
     * constructor
     * @param nodeName
     */
    public CommitResult(String nodeName) {
        super(nodeName);
    }

    public void addShardException(String shardName, Exception exception) {
        exceptionByShard.put(shardName, exception);
    }

    public void addShardMetaDataMap(String shardName, Map<String, String> shardMetaData) {
        metadataMapByShard.put(shardName, shardMetaData);
    }

    public Map<String, Exception> getShardExceptions() {
        return exceptionByShard;
    }

    public Map<String, Map<String, String>> getShardMetaDataMaps() {
        return metadataMapByShard;
    }

}
