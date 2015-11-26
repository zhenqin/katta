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
package com.ivyft.katta.master;

import com.ivyft.katta.operation.OperationId;
import com.ivyft.katta.operation.master.IndexDeployOperation;
import com.ivyft.katta.operation.master.MasterOperation;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.protocol.metadata.Shard;
import com.ivyft.katta.util.CircularList;
import com.ivyft.katta.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Simple deploy policy which distributes the shards in round robin style.<b>
 * Following features are supported:<br>
 * - initial shard distribution<br>
 * - shard distribution when under replicated<br>
 * - shard removal when over-replicated <br>
 * <p/>
 * <p/>
 * Missing feature:<br>
 * - shard/node rebalancing<br>
 * <p/>
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
public class DefaultDistributionPolicy implements IDeployPolicy {

    private final static Logger LOG = LoggerFactory.getLogger(DefaultDistributionPolicy.class);


    /**
     * 必须有默认的构造方法。用反射实例化该对象的
     */
    public DefaultDistributionPolicy() {

    }

    public Map<String, List<String>> createDistributionPlan(Map<String, List<String>> currentShard2NodesMap,
                                                            Map<String, List<String>> currentNode2ShardsMap,
                                                            List<String> aliveNodes,
                                                            int replicationLevel) {
        if (aliveNodes.size() == 0) {
            throw new IllegalArgumentException("no alive nodes to distribute to");
        }

        //TODO 核心代码，目前还不清楚shard分发逻辑，待测试
        Set<String> shards = currentShard2NodesMap.keySet();
        for (String shard : shards) {
            Set<String> assignedNodes = new HashSet<String>(replicationLevel);
            int neededDeployments = replicationLevel - countValues(currentShard2NodesMap, shard);
            assignedNodes.addAll(currentShard2NodesMap.get(shard));

            // now assign new nodes based on round robin algorithm
            sortAfterFreeCapacity(aliveNodes, currentNode2ShardsMap);
            CircularList<String> roundRobinNodes = new CircularList<String>(aliveNodes);
            neededDeployments = chooseNewNodes(currentNode2ShardsMap, roundRobinNodes, shard, assignedNodes,
                    neededDeployments);

            if (neededDeployments > 0) {
                LOG.warn("cannot replicate shard '" + shard + "' " + replicationLevel + " times, cause only "
                        + roundRobinNodes.size() + " nodes connected");
            } else if (neededDeployments < 0) {
                LOG.info("found shard '" + shard + "' over-replicated");
                // over replication is ok ?
                removeOverreplicatedShards(currentShard2NodesMap, currentNode2ShardsMap, shard, neededDeployments);
            }
        }
        return currentNode2ShardsMap;
    }

    private void sortAfterFreeCapacity(List<String> aliveNodes, final Map<String, List<String>> node2ShardsMap) {
        Collections.sort(aliveNodes, new Comparator<String>() {
            @Override
            public int compare(String node1, String node2) {
                int size1 = node2ShardsMap.get(node1).size();
                int size2 = node2ShardsMap.get(node2).size();
                return (size1 < size2 ? -1 : (size1 == size2 ? 0 : 1));
            }
        });
    }

    private int chooseNewNodes(final Map<String, List<String>> currentNode2ShardsMap,
                               CircularList<String> roundRobinNodes, String shard, Set<String> assignedNodes, int neededDeployments) {
        String tailNode = roundRobinNodes.getTail();
        String currentNode = null;
        while (neededDeployments > 0 && !tailNode.equals(currentNode)) {
            currentNode = roundRobinNodes.getNext();
            if (!assignedNodes.contains(currentNode)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("assign " + shard + " to " + currentNode);
                }
                currentNode2ShardsMap.get(currentNode).add(shard);
                assignedNodes.add(currentNode);
                neededDeployments--;
            }
        }
        return neededDeployments;
    }

    private void removeOverreplicatedShards(final Map<String, List<String>> currentShard2NodesMap,
                                            final Map<String, List<String>> currentNode2ShardsMap, String shard, int neededDeployments) {
        while (neededDeployments < 0) {
            int maxShardServingCount = 0;
            String maxShardServingNode = null;
            List<String> nodeNames = currentShard2NodesMap.get(shard);
            for (String node : nodeNames) {
                int shardCount = countValues(currentNode2ShardsMap, node);
                if (shardCount > maxShardServingCount) {
                    maxShardServingCount = shardCount;
                    maxShardServingNode = node;
                }
            }
            currentNode2ShardsMap.get(maxShardServingNode).remove(shard);
            neededDeployments++;
        }
    }

    private int countValues(Map<String, List<String>> multiMap, String key) {
        List<String> list = multiMap.get(key);
        if (list == null) {
            return 0;
        }
        return list.size();
    }





    public static void main(String[] args) throws Exception {
        ZkClient zkClient = new ZkClient("zhenqin-pro102:2181", 6000, 6000);
        InteractionProtocol protocol = new InteractionProtocol(zkClient, new ZkConfiguration());
        IndexDeployOperation operation = new IndexDeployOperation("test", "path", "core", 2);

        IndexMetaData data = new IndexMetaData("test", "path", "core", 4);
        data.addShard(new Shard("shard1", "path1"));
        data.addShard(new Shard("shard2", "path2"));
        data.addShard(new Shard("shard3", "path3"));

        List<String> nodes = new ArrayList<String>();
        for (int i = 0; i < 5; i++) {
            nodes.add("node-" + i);
        }
        MasterContext context = new MasterContext(protocol, null, new DefaultDistributionPolicy(), null);

        List<OperationId> ids = operation.distributeIndexShards(context, data, nodes, Arrays.<MasterOperation>asList(operation));
        System.out.println(ids);
    }
}
