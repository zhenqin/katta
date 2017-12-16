/**
 * Copyright 2009 the original author or authors.
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
package com.ivyft.katta.client;

import com.ivyft.katta.lib.lucene.ILuceneServer;
import com.ivyft.katta.lib.lucene.QueryWritable;
import com.ivyft.katta.util.KattaException;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.solr.client.solrj.SolrQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 * 该类用反射完成调用, 性能低下. 日后需要调整
 *
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
class NodeInteraction<T> implements Runnable {


    // Used to make logs easier to read.
    private static int interactionInstanceCounter;

    private final Method method;
    private final Object[] args;


    private final int shardArrayIndex;
    private final String node;
    private final Map<String, List<String>> node2ShardsMap;


    private final List<String> shards;
    private final int tryCount;
    private final int maxTryCount;


    private final INodeExecutor workQueue;
    private final INodeProxyManager shardManager;


    private final IResultReceiver<T> result;


    private final int instanceId = interactionInstanceCounter++;


    private static final Logger log = LoggerFactory.getLogger(NodeInteraction.class);


    
    /**
     * Create a node interaction. This will make one call to one node, listing
     * multiple shards.
     *
     * @param method          Which method to call on the server. This method must have come
     *                        from the same interface used to creat the RPC proxy in the first
     *                        place (see Client constructor).
     * @param args            The arguments to pass to the method. When calling the server, the
     *                        shard list argument will be modified if shardArrayIndex >= 0.
     * @param shardArrayIndex Which parameter, if any, to overwrite with a String[] of shard
     *                        names (this one arg then changes an a per-node basis, otherwise
     *                        all arguments are the same for all calls to nodes). This is
     *                        optional, depending on the needs of the server. When < 0, no
     *                        overwriting is done.
     * @param node            The name of the node to contact. This is used to get the node's
     *                        proxy object (see IShardProxyManager).
     * @param node2ShardsMap  The mapping from nodes to shards for all nodes. Used initially
     *                        with the node "node", but other nodes if errors occur and we
     *                        retry. For every retry the failed node is removed from the map and
     *                        the map is then used to submit a retry job.
     * @param tryCount        This interaction is the Nth retry (starts at 1). We use this to
     *                        decide if we should retry shards.
     * @param maxTryCount     The maximum number a interaction is tried to be executed.
     * @param shardManager    Our source of node proxies, and access to a node selection policy
     *                        to pick the nodes to use for retires. Also we notify this object
     *                        on node failures.
     * @param workQueue       Use this if we need to resubmit a retry job. Will result in a new
     *                        NodeInteraction.
     * @result The destination to write to. If we get a result from the node we
     * add it. If we get an error and submit retries we do not use it (the
     * retry jobs will write to it for us). If we get an error and do not
     * retry we write the error to it.
     */
    public NodeInteraction(Method method,
                           Object[] args,
                           int shardArrayIndex,
                           String node,
                           Map<String, List<String>> node2ShardsMap,
                           int tryCount,
                           int maxTryCount,
                           INodeProxyManager shardManager,
                           INodeExecutor workQueue,
                           IResultReceiver<T> result) {
        this.method = method;
        // Make a copy in case we will be modifying the shard list.
        this.args = Arrays.copyOf(args, args.length);
        this.shardArrayIndex = shardArrayIndex;
        this.node = node;
        this.node2ShardsMap = node2ShardsMap;
        this.shards = node2ShardsMap.get(node);
        this.tryCount = tryCount;
        this.maxTryCount = maxTryCount;
        this.workQueue = workQueue;
        this.shardManager = shardManager;
        this.result = result;
    }



    public void run() {
        try {
            VersionedProtocol proxy = shardManager.getProxy(this.node, false);
            if (proxy == null) {
                throw new KattaException("No proxy for node: " + this.node);
            }

            ILuceneServer luceneServer = (ILuceneServer)proxy;

            if (this.shardArrayIndex >= 0) {
                // We need to pass the list of shards to the server's method.
                args[this.shardArrayIndex] = this.shards.toArray(new String[this.shards.size()]);
            }
            long startTime = System.currentTimeMillis();
            //反射完成调用

            T result = (T) this.method.invoke(luceneServer, args);

            shardManager.reportNodeCommunicationSuccess(this.node);

            log.info("exec {} shards {} method {} success, use time: {} ms",
                    node,
                    shards.toString(),
                    method.getName(),
                    (System.currentTimeMillis() - startTime));

            //TODO 远程调用完成, 需要合并结果
            this.result.addResult(result, this.shards);

            synchronized (this.result) {
                this.result.notifyAll();
            }
        } catch (Throwable t) {
            // Notify the work queue, so it can mark the node as down.
            shardManager.reportNodeCommunicationFailure(this.node, t);
            if (tryCount >= maxTryCount) {
                log.error(String.format("Error calling %s (try # %d of %d) (id=%d)", (this.method
                        + " on " + this.node), tryCount, maxTryCount, instanceId), t);
                result.addError(new KattaException(String.format("%s for shards %s failed (id=%d)",
                        getClass().getSimpleName(), shards, instanceId), t), shards);
                return;
            }
            if (!result.isClosed()) {
                try {
                    // Find new node(s) for our shards and add to global node2ShardMap
                    Map<String, List<String>> retryMap = shardManager.createNode2ShardsMap(node2ShardsMap.get(this.node));
                    log.warn(String.format("Failed to interact with node %s. Trying with other node(s) %s (id=%d)", this.node,
                            retryMap.keySet(), instanceId), t);
                    // Execute the action again for every node
                    for (String newNode : retryMap.keySet()) {
                        workQueue.execute(newNode, retryMap, tryCount + 1, maxTryCount);
                    }
                } catch (ShardAccessException e) {
                    log.error(String.format("Error calling %s (try # %d of %d) (id=%d)",
                            (this.method + " on " + this.node), tryCount, maxTryCount, instanceId), t);
                    result.addError(e, shards);
                }
            } else {
                log.error(String.format("Error after results closed for call to %s (try # %d of %d; giving up) (id=%d)",
                        (this.method + " on " + this.node), tryCount, maxTryCount, instanceId), t);
            }
            // We have no results to report. Submitted jobs will hopefully get results
            // instead.
        }
    }

    private String describeMethodCall(Method method, Object[] args, String nodeName) {
        StringBuilder builder = new StringBuilder(method.getDeclaringClass().getSimpleName());
        builder.append(".");
        builder.append(method.getName());
        builder.append("(");
        String sep = "";
        for (int i = 0; i < args.length; i++) {
            builder.append(sep);
            if (args[i] == null) {
                builder.append("null");
            } else if (args[i] instanceof String[]) {
                String[] strs = (String[]) args[i];
                String sep2 = "";
                builder.append("[");
                for (String str : strs) {
                    builder.append(sep2 + "\"" + str + "\"");
                    sep2 = ", ";
                }
                builder.append("]");
            } else {
                builder.append(args[i].toString());
            }
            sep = ", ";
        }
        builder.append(") on ");
        builder.append(nodeName);
        return builder.toString();
    }

    private String resultToString(T result) {
        String s = "null";
        if (result != null) {
            try {
                s = result.toString();
            } catch (Throwable t) {
                log.trace("Error calling toString() on result", t);
                s = "(toString() err)";
            }
        }
        if (s == null) {
            s = "(null toString())";
        }
        return s;
    }

    @Override
    public String toString() {
        return "NodeInteraction: call " + this.method.getName() + " on " + this.node;
    }
}
