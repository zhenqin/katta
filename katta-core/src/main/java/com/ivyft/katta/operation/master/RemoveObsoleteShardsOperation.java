/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ivyft.katta.operation.master;

import com.ivyft.katta.master.MasterContext;
import com.ivyft.katta.operation.OperationId;
import com.ivyft.katta.operation.node.OperationResult;
import com.ivyft.katta.operation.node.ShardUndeployOperation;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 *
 * <p>
 *     该类是从一个节点上移除一个shard的操作。
 * </p>
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-13
 * Time: 上午8:58
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class RemoveObsoleteShardsOperation implements MasterOperation {


    /**
     * 序列化表格
     */
    private static final long serialVersionUID = 1L;


    /**
     * 节点名称
     */
    private final String nodeName;


    /**
     * log
     */
    protected final static Logger LOG = LoggerFactory.getLogger(RemoveObsoleteShardsOperation.class);


    /**
     * 节点失效， 要移除该节点
     * @param nodeName 节点名称
     */
    public RemoveObsoleteShardsOperation(String nodeName) {
        this.nodeName = nodeName;
    }


    @Override
    public List<OperationId> execute(MasterContext context,
                                     List<MasterOperation> runningOperations) throws Exception {
        InteractionProtocol protocol = context.getProtocol();

        //获得指定node的shard集合
        Collection<String> nodeShards = protocol.getNodeShards(this.nodeName);
        LOG.info("node: " + nodeName + " deploy shards: " + nodeShards);

        //卸载那些没有使用的Shard
        List<String> obsoletShards = collectObsoleteShards(protocol, nodeShards, runningOperations);
        if (!obsoletShards.isEmpty()) {
            Log.info("found following shards obsolete on node " + this.nodeName + ": " + obsoletShards);

            //给它发送卸载shard的消息
            protocol.addNodeOperation(this.nodeName, new ShardUndeployOperation(obsoletShards));
        }

        return null;
    }


    @Override
    public void nodeOperationsComplete(MasterContext context,
                                       List<OperationResult> results) throws Exception {
        // nothing to do
    }

    @Override
    public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
        return ExecutionInstruction.EXECUTE;
    }


    /**
     *
     * @param protocol zk 操作
     * @param nodeShards 指定node的shard集合
     * @param runningOperations ope
     * @return
     */
    private List<String> collectObsoleteShards(InteractionProtocol protocol,
                                               Collection<String> nodeShards,
                                               List<MasterOperation> runningOperations) {
        List<String> obsoletShards = new ArrayList<String>();
        for (String shardName : nodeShards) {
            try {
                String indexName = AbstractIndexOperation.getIndexNameFromShardName(shardName);
                IndexMetaData indexMD = protocol.getIndexMD(indexName);

                if (indexMD == null && !containsDeployOperation(runningOperations, indexName)) {
                    // index has been removed
                    obsoletShards.add(shardName);
                }
            } catch (IllegalArgumentException e) {
                Log.warn("found shard with invalid name '" + shardName + "' - instruct removal");
                obsoletShards.add(shardName);
            }
        }
        return obsoletShards;
    }


    private boolean containsDeployOperation(List<MasterOperation> runningOperations,
                                            String indexName) {
        for (MasterOperation masterOperation : runningOperations) {
            if (masterOperation instanceof IndexDeployOperation
                    && ((IndexDeployOperation) masterOperation).getIndexName().equals(indexName)) {
                return true;
            }
        }
        return false;
    }


    public String getNodeName() {
        return this.nodeName;
    }


    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + Integer.toHexString(hashCode()) + ":" + this.nodeName;
    }

}
