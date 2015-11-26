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

import java.util.List;
import java.util.Map;

/**
 * <p>
 *     该类用来创建分布式索引分发计划，子类可实现
 *     {@link #createDistributionPlan(Map, Map, List, int)}
 *     方法
 * </p>
 * <p>
 *
 * Exchangeable policy for which creates an distribution plan for the shards of
 * one index.
 *
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
public interface IDeployPolicy {

    /**
     * Creates a distribution plan for the shards of one index. Note that the
     * index can already be deployed, in that case its more a "replication" plan.
     *
     * @param currentShard2NodesMap all current deployments of the shards of the one index to
     *                              distribute/replicate
     * @param currentNode2ShardsMap all nodes and their shards
     * @param aliveNodes all live Nodes
     * @param replicationLevel replication level
     * @return 返回Node-shard计划
     */
    Map<String, List<String>> createDistributionPlan(Map<String, List<String>> currentShard2NodesMap,
                                                     Map<String, List<String>> currentNode2ShardsMap,
                                                     List<String> aliveNodes,
                                                     int replicationLevel);

}
