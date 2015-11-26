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
import com.ivyft.katta.operation.node.DeployResult;
import com.ivyft.katta.operation.node.OperationResult;
import com.ivyft.katta.operation.node.ShardUndeployOperation;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.ReplicationReport;
import com.ivyft.katta.protocol.metadata.IndexDeployError;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.protocol.metadata.Shard;
import com.ivyft.katta.util.CollectionUtil;
import com.ivyft.katta.util.One2ManyListMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;


/**
 *
 *
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-19
 * Time: 上午8:59
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public abstract class AbstractIndexOperation implements MasterOperation {

    /**
     * 序列化表格
     */
    private static final long serialVersionUID = 1L;


    /**
     * 分割符
     */
    public static final char INDEX_SHARD_NAME_SEPARATOR = '#';


    /**
     * 每个shard加载的node
     */
    private Map<String, List<String>> newShardsByNodeMap = new HashMap<String, List<String>>();


    /**
     * Log
     */
    private static final Logger LOG = LoggerFactory.getLogger(AbstractIndexOperation.class);


    /**
     * 默认构造方法
     */
    protected AbstractIndexOperation() {

    }


    /**
     * 发布计划?
     * @param context
     * @param indexMD
     * @param liveNodes
     * @param runningOperations
     * @return
     * @throws IndexDeployException
     */
    public synchronized List<OperationId> distributeIndexShards(MasterContext context,
                                                                IndexMetaData indexMD,
                                                                Collection<String> liveNodes,
                                                                List<MasterOperation> runningOperations)
            throws IndexDeployException {

        //创建分布式的索引计划

        if (liveNodes.isEmpty()) {
            throw new IndexDeployException(IndexDeployError.ErrorType.NO_NODES_AVAILIBLE, "no nodes availible");
        }

        InteractionProtocol protocol = context.getProtocol();
        Set<Shard> shards = indexMD.getShards();

        //这一步检验每个Shard被几个Node所加载
        Map<String, List<String>> currentIndexShard2NodesMap = protocol
                .getShard2NodesMap(Shard.getShardNames(shards));
        //{shard1=[node1, node2], shard2=[node2, node3]}

        //这里检验每个Node上有几个shard
        Map<String, List<String>> currentGlobalNode2ShardsMap = getCurrentNode2ShardMap(liveNodes, protocol
                .getShard2NodesMap(protocol.getShard2NodeShards()));
        //{node1=[index1#shard1, index2#shard3], node2:20000=[test#index1, test#index2, test#index]}

        addRunningDeployments(currentGlobalNode2ShardsMap, runningOperations);

        //创建分布式索引分发计划
        Map<String, List<String>> newNode2ShardMap = context.getDeployPolicy().createDistributionPlan(
                currentIndexShard2NodesMap,
                cloneMap(currentGlobalNode2ShardsMap),
                new ArrayList<String>(liveNodes),
                indexMD.getReplicationLevel());

        /*
        boolean stop = true;
        if(stop) {
            return null;
        }
        */

        LOG.info("new deploy plan: " + newNode2ShardMap.toString());

        // node to shards
        Set<String> nodes = newNode2ShardMap.keySet();
        List<OperationId> operationIds = new ArrayList<OperationId>(nodes.size());


        One2ManyListMap<String, String> newShardsByNode = new One2ManyListMap<String, String>();
        for (String node : nodes) {
            List<String> nodeShards = newNode2ShardMap.get(node);
            List<String> listOfAdded = CollectionUtil.getListOfAdded(
                    currentGlobalNode2ShardsMap.get(node), nodeShards);
            if (!listOfAdded.isEmpty()) {
                com.ivyft.katta.operation.node.ShardDeployOperation deployInstruction =
                        new com.ivyft.katta.operation.node.ShardDeployOperation();

                for (String shard : listOfAdded) {
                    //这里还要添加上collectionName
                    deployInstruction.addShard(shard, indexMD.getShardPath(shard), indexMD.getCollectionName());
                    newShardsByNode.add(node, shard);
                    LOG.info(node + " add shard: " + shard);
                }
                OperationId operationId = protocol.addNodeOperation(node, deployInstruction);
                operationIds.add(operationId);
            }
            List<String> listOfRemoved = CollectionUtil.getListOfRemoved(
                    currentGlobalNode2ShardsMap.get(node), nodeShards);
            if (!listOfRemoved.isEmpty()) {
                ShardUndeployOperation undeployInstruction = new ShardUndeployOperation(listOfRemoved);
                LOG.info(node + " remove shard: " + StringUtils.join(listOfRemoved, ", "));
                OperationId operationId = protocol.addNodeOperation(node, undeployInstruction);
                operationIds.add(operationId);
            }
        }
        newShardsByNodeMap = newShardsByNode.asMap();
        return operationIds;
    }


    /**
     * 防止currentNode2ShardsMap中得值有null的现象，合并两个Map中得值
     * @param currentNode2ShardsMap
     * @param runningOperations
     */
    private void addRunningDeployments(Map<String, List<String>> currentNode2ShardsMap,
                                       List<MasterOperation> runningOperations) {
        for (MasterOperation masterOperation : runningOperations) {
            if (masterOperation instanceof AbstractIndexOperation) {
                AbstractIndexOperation indexOperation = (AbstractIndexOperation) masterOperation;
                for (Entry<String, List<String>> entry : indexOperation.getNewShardsByNodeMap().entrySet()) {
                    List<String> shardList = currentNode2ShardsMap.get(entry.getKey());
                    if (shardList == null) {
                        shardList = new ArrayList<String>(entry.getValue().size());
                        currentNode2ShardsMap.put(entry.getKey(), shardList);
                    }
                    shardList.addAll(entry.getValue());
                }
            }
        }
    }

    private Map<String, List<String>> cloneMap(Map<String, List<String>> currentShard2NodesMap) {
        // return currentShard2NodesMap;
        Set<Entry<String, List<String>>> entries = currentShard2NodesMap.entrySet();
        HashMap<String, List<String>> clonedMap = new HashMap<String, List<String>>();
        for (Entry<String, List<String>> e : entries) {
            clonedMap.put(e.getKey(), new ArrayList<String>(e.getValue()));
        }
        return clonedMap;
    }

    private Map<String, List<String>> getCurrentNode2ShardMap(Collection<String> liveNodes,
                                                              Map<String, List<String>> currentShard2NodesMap) {
        Map<String, List<String>> currentNodeToShardsMap =
                CollectionUtil.invertListMap(currentShard2NodesMap);
        for (String node : liveNodes) {
            if (!currentNodeToShardsMap.containsKey(node)) {
                currentNodeToShardsMap.put(node, new ArrayList<String>(3));
            }
        }
        return currentNodeToShardsMap;
    }

    protected boolean canAndShouldRegulateReplication(InteractionProtocol protocol,
                                                      IndexMetaData indexMD) {
        ReplicationReport replicationReport = protocol.getReplicationReport(indexMD);
        return canAndShouldRegulateReplication(protocol, replicationReport);
    }

    protected boolean canAndShouldRegulateReplication(InteractionProtocol protocol,
                                                      ReplicationReport replicationReport) {
        List<String> liveNodes = protocol.getLiveNodes();
        if (replicationReport.isBalanced()) {
            return false;
        }
        if (replicationReport.isUnderreplicated()
                && liveNodes.size() <= replicationReport.getMinimalShardReplicationCount()) {
            return false;
        }
        return true;
    }


    /**
     * 把异常的索引Shard信息写入ZooKeeper
     * @param protocol
     * @param indexMD
     * @param e
     */
    protected void handleMasterDeployException(InteractionProtocol protocol,
                                               IndexMetaData indexMD,
                                               Exception e) {
        IndexDeployError.ErrorType errorType;
        if (e instanceof IndexDeployException) {
            errorType = ((IndexDeployException) e).getErrorType();
        } else {
            errorType = IndexDeployError.ErrorType.UNKNOWN;
        }
        IndexDeployError deployError = new IndexDeployError(indexMD.getName(), errorType);
        deployError.setException(e);
        indexMD.setDeployError(deployError);
        protocol.updateIndexMD(indexMD);
    }


    /**
     * 部署成功后，发布Index
     * @param context Context
     * @param results 部署结果
     * @param indexMD 索引元信息
     * @param newIndex 是更新还是直接插入
     */
    protected void handleDeploymentComplete(MasterContext context,
                                            List<OperationResult> results,
                                            IndexMetaData indexMD,
                                            boolean newIndex) {
        ReplicationReport replicationReport = context.getProtocol().getReplicationReport(indexMD);
        if (replicationReport.isDeployed()) {
            //部署成功
            indexMD.setDeployError(null);
            updateShardMetaData(results, indexMD);
            // we ignore possible shard errors
            if (canAndShouldRegulateReplication(context.getProtocol(), replicationReport)) {
                context.getProtocol().addMasterOperation(new BalanceIndexOperation(indexMD.getName()));
            }
        } else {
            //部署失败写入异常
            IndexDeployError deployError = new IndexDeployError(indexMD.getName(),
                    IndexDeployError.ErrorType.SHARDS_NOT_DEPLOYABLE);
            for (OperationResult operationResult : results) {
                if (operationResult != null) {// node-crashed produces null
                    DeployResult deployResult = (DeployResult) operationResult;
                    for (Entry<String, Exception> entry : deployResult.getShardExceptions().entrySet()) {
                        deployError.addShardError(entry.getKey(), entry.getValue());
                    }
                }
            }
            indexMD.setDeployError(deployError);
        }
        if (newIndex) {
            context.getProtocol().publishIndex(indexMD);
        } else {
            context.getProtocol().updateIndexMD(indexMD);
        }
    }


    /**
     * 更新Index Shard的元信息
     * @param results 部署Result
     * @param indexMD Index Meta Data
     */
    private void updateShardMetaData(List<OperationResult> results, IndexMetaData indexMD) {
        for (OperationResult operationResult : results) {
            if (operationResult != null) {// node-crashed produces null
                DeployResult deployResult = (DeployResult) operationResult;
                for (Entry<String, Map<String, String>> entry : deployResult.getShardMetaDataMaps().entrySet()) {
                    Map<String, String> existingMap = indexMD.getShard(entry.getKey()).getMetaDataMap();
                    Map<String, String> newMap = entry.getValue();
                    if (existingMap.size() > 0 && !existingMap.equals(newMap)) {
                        // maps from different nodes but for the same shard should have
                        // the same content
                        LOG.warn("new shard metadata differs from existing one. old: " + existingMap + " new: " + newMap);
                    }
                    existingMap.putAll(newMap);
                }
            }
        }
    }


    protected Map<String, List<String>> getNewShardsByNodeMap() {
        return newShardsByNodeMap;
    }


    /**
     * 根据索引目录名称创建一个shardName
     * @param indexName 索引目录名称
     * @param shardPath Path
     * @return
     */
    public static String createShardName(String indexName, String shardPath) {
        int lastIndexOf = shardPath.lastIndexOf("/");
        if (lastIndexOf == -1) {
            lastIndexOf = 0;
        }
        String shardFolderName = shardPath.substring(lastIndexOf + 1, shardPath.length());
        if (shardFolderName.endsWith(".zip")) {
            shardFolderName = shardFolderName.substring(0, shardFolderName.length() - 4);
        }
        return indexName + INDEX_SHARD_NAME_SEPARATOR + shardFolderName;
    }


    /**
     * 从node加载的shard判断shardName
     * @param shardIndexDirName node记载的shard整个路径
     * @return 返回shardName
     */
    public static String getIndexNameFromShardName(String shardIndexDirName) {
        try {
            return shardIndexDirName.substring(0, shardIndexDirName.indexOf(INDEX_SHARD_NAME_SEPARATOR));
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(shardIndexDirName + " is not a valid shard name");
        }
    }

}
