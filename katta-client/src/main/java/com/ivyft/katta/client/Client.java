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
package com.ivyft.katta.client;

import com.ivyft.katta.protocol.ConnectedComponent;
import com.ivyft.katta.protocol.IAddRemoveListener;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.protocol.metadata.Shard;
import com.ivyft.katta.util.*;
import com.ivyft.katta.util.ZkConfiguration.PathDef;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;


/**
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
public class Client implements ConnectedComponent {


    /**
     * 所有的shard
     */
    private static final String[] ALL_INDICES = new String[]{"*"};



    /**
     * 启动时间
     */
    private final long STARTUP_TIME;


    /**
     * 复制集。 不加入搜索
     */
    protected final Set<String> indicesToWatch = new HashSet<String>();


    /**
     * 分片集。 需要被搜索
     */
    protected final Map<String, List<String>> indexToShards = new ConcurrentHashMap<String, List<String>>();


    /**
     * 访问计数
     */
    private long queryCount = 0;


    private final int maxTryCount;


    protected final ExecutorService executor;



    private final ClientConfiguration clientConfiguration;


    protected InteractionProtocol protocol;


    protected final INodeSelectionPolicy selectionPolicy;


    private INodeProxyManager proxyManager;


    /**
     * log
     */
    protected final static Logger log = LoggerFactory.getLogger(Client.class);


    /**
     * constructor
     * @param serverClass
     */
    public Client(Class<? extends VersionedProtocol> serverClass) {
        this(serverClass, new ShuffleNodeSelectionPolicy(), new ZkConfiguration());
    }

    public Client(Class<? extends VersionedProtocol> serverClass, final ZkConfiguration config) {
        this(serverClass, new ShuffleNodeSelectionPolicy(), config);
    }

    public Client(Class<? extends VersionedProtocol> serverClass, InteractionProtocol protocol) {
        this(serverClass, new ShuffleNodeSelectionPolicy(), protocol, new ClientConfiguration());
    }

    public Client(Class<? extends VersionedProtocol> serverClass, final INodeSelectionPolicy nodeSelectionPolicy) {
        this(serverClass, nodeSelectionPolicy, new ZkConfiguration());
    }

    public Client(Class<? extends VersionedProtocol> serverClass, final INodeSelectionPolicy policy,
                  final ZkConfiguration zkConfig) {
        this(serverClass, policy, zkConfig, new ClientConfiguration());
    }

    public Client(Class<? extends VersionedProtocol> serverClass,
                  final INodeSelectionPolicy policy,
                  final ZkConfiguration zkConfig,
                  ClientConfiguration clientConfiguration) {
        this(serverClass, policy,
                new InteractionProtocol(ZkKattaUtil.startZkClient(zkConfig, 60000), zkConfig),
                clientConfiguration);
    }

    public Client(Class<? extends VersionedProtocol> serverClass,
                  final INodeSelectionPolicy policy,
                  final InteractionProtocol protocol,
                  ClientConfiguration clientConfiguration) {
        Set<String> keys = new HashSet<String>(clientConfiguration.getKeys());
        Configuration hadoopConf = HadoopUtil.getHadoopConf();
        synchronized (Configuration.class) {// fix for KATTA-146
            for (String key : keys) {
                // simply set all properties / adding non-hadoop properties shouldn't
                // hurt
                hadoopConf.set(key, clientConfiguration.getProperty(key));
            }
        }


        this.proxyManager = new NodeProxyManager(serverClass, hadoopConf, policy);
        this.selectionPolicy = policy;
        this.protocol = protocol;
        this.clientConfiguration = clientConfiguration;

        this.maxTryCount = this.clientConfiguration.getInt(ClientConfiguration.CLIENT_NODE_INTERACTION_MAXTRYCOUNT);
        int threadCount = this.clientConfiguration.getInt(ClientConfiguration.CLIENT_THEAD_POOL_CAPACITY);
        this.executor = Executors.newScheduledThreadPool(threadCount);

        List<String> indexList = this.protocol.registerChildListener(this,
                PathDef.INDICES_METADATA,
                new IAddRemoveListener() {


                    @Override
                    public void removed(String name) {
                        removeIndex(name);
                    }

                    @Override
                    public void added(String name) {
                        IndexMetaData indexMD = protocol.getIndexMD(name);
                        if (isIndexSearchable(indexMD)) {
                            addIndexForSearching(indexMD);
                        } else {
                            addIndexForWatching(name);
                        }
                    }
        });

        log.info("indices=" + indexList);

        //把可搜索的shard加入到搜索列表
        addOrWatchNewIndexes(indexList);

        //启动时间
        STARTUP_TIME = System.currentTimeMillis();
    }

    public INodeSelectionPolicy getSelectionPolicy() {
        return this.selectionPolicy;
    }

    public INodeProxyManager getProxyManager() {
        return this.proxyManager;
    }

    public void setProxyCreator(INodeProxyManager proxyManager) {
        this.proxyManager = proxyManager;
    }

    protected void removeIndexes(List<String> indexes) {
        for (String index : indexes) {
            removeIndex(index);
        }
    }

    protected void removeIndex(String index) {
        List<String> shards = indexToShards.remove(index);
        if (shards != null) {
            for (String shard : shards) {
                try {
                    this.selectionPolicy.remove(shard);
                } catch (ShardAccessException e) {
                    log.warn("Could not remove shard", e);
                }
                this.protocol.unregisterChildListener(this, PathDef.SHARD_TO_NODES, shard);
            }
        } else {
            if (indicesToWatch.contains(index)) {
                this.protocol.unregisterDataChanges(this, PathDef.INDICES_METADATA, index);
            } else {
                log.warn("got remove event for index '" + index + "' but have no shards for it");
            }
        }
    }

    protected void addOrWatchNewIndexes(List<String> indexes) {
        for (String index : indexes) {
            IndexMetaData indexMD = this.protocol.getIndexMD(index);
            if (indexMD != null) {
                // could be undeployed in meantime
                //是否是可搜索的？ 有可能这是一个复制节点
                if (isIndexSearchable(indexMD)) {
                    //加入搜索列表
                    addIndexForSearching(indexMD);
                } else {
                    //复制节点加入监控列表
                    addIndexForWatching(index);
                }
            }
        }
    }

    protected void addIndexForWatching(final String indexName) {
        indicesToWatch.add(indexName);
        this.protocol.registerDataListener(
                this,
                PathDef.INDICES_METADATA,
                indexName,
                new IZkDataListener() {
                    @Override
                    public void handleDataDeleted(String dataPath) throws Exception {
                        // handled through IndexPathListener
                    }

                    @Override
                    public void handleDataChange(String dataPath, Object data) throws Exception {
                        IndexMetaData metaData = (IndexMetaData) data;
                        if (isIndexSearchable(metaData)) {
                            addIndexForSearching(metaData);
                            protocol.unregisterDataChanges(Client.this, dataPath);
                        }
                    }
                });
    }

    protected void addIndexForSearching(IndexMetaData indexMD) {
        final Set<Shard> shards = indexMD.getShards();
        List<String> shardNames = new ArrayList<String>();
        for (Shard shard : shards) {
            shardNames.add(shard.getName());
        }
        for (final String shardName : shardNames) {
            List<String> nodes = this.protocol.registerChildListener(
                    this,
                    PathDef.SHARD_TO_NODES,
                    shardName,
                    new IAddRemoveListener() {
                        @Override
                        public void removed(String nodeName) {
                            Collection<String> shardNodes = null;
                            try {
                                shardNodes = new ArrayList<String>(selectionPolicy.getShardNodes(shardName));
                                shardNodes.remove(nodeName);

                                selectionPolicy.update(shardName, shardNodes);
                                log.info("shard '" + shardName + "' removed from node " + nodeName + "'");
                            } catch (ShardAccessException e) {
                                // no-op - the selection policy is already unaware of this shard, so nothing to remove
                            }
                        }

                        @Override
                        public void added(String nodeName) {
                            VersionedProtocol proxy = proxyManager.getProxy(nodeName, true);
                            if (proxy != null) {
                                try {
                                    Collection<String> shardNodes = new ArrayList<String>(selectionPolicy.getShardNodes(shardName));
                                    shardNodes.add(nodeName);
                                    selectionPolicy.update(shardName, shardNodes);
                                    log.info("shard '" + shardName + "' added to node '" + nodeName + "'");
                                } catch (ShardAccessException e) {
                                    log.warn("Could not add shard '" + shardName + "' to node '" + nodeName + "'", e);
                                }
                            }
                        }
                    });


            Collection<String> shardNodes = new ArrayList<String>(3);
            for (String node : nodes) {
                VersionedProtocol proxy = proxyManager.getProxy(node, true);
                if (proxy != null) {
                    shardNodes.add(node);
                }
            }
            selectionPolicy.update(shardName, shardNodes);
        }
        indexToShards.put(indexMD.getName(), shardNames);
    }



    protected boolean isIndexSearchable(final IndexMetaData indexMD) {
        //这里默认的规则是有异常就不能搜索
        if (indexMD.hasDeployError()) {
            return false;
        }
        // TODO 判断这个Node是否能被搜索 ?
        return true;
    }





    // --------------- Distributed calls to servers ----------------------

    /*
     * Broadcast a method call to all indices. Return all the results in a
     * Collection.
     *
     * @param <T>
     * @param timeout
     * @param shutdown
     * @param method
     *          The server's method to call.
     * @param shardArrayParamIndex
     *          Which parameter of the method call, if any, that should be
     *          replaced with the shards to search. This is an array of Strings,
     *          with a different value for each node / server. Pass in -1 to
     *          disable.
     * @param args
     *          The arguments to pass to the method when run on the server.
     * @return the results
     * @throws KattaException
     */
    public <T> IResultReceiver<T> broadcastToAll(long timeout,
                                              boolean shutdown,
                                              Method method,
                                              int shardArrayParamIndex,
                                              Object... args) throws KattaException {
        return broadcastToAll(new ResultCompletePolicy<T>(timeout, shutdown), method, shardArrayParamIndex, args);
    }

    public <T> IResultReceiver<T> broadcastToAll(IResultPolicy<T> resultPolicy,
                                              Method method,
                                              int shardArrayParamIndex,
                                              Object... args) throws KattaException {
        return broadcastToShards(resultPolicy, method, shardArrayParamIndex, null, args);
    }

    public <T> IResultReceiver<T> broadcastToIndices(long timeout,
                                                  boolean shutdown,
                                                  Method method,
                                                  int shardArrayIndex,
                                                  String[] indices,
                                                  Object... args) throws KattaException {
        return broadcastToIndices(new ResultCompletePolicy<T>(timeout, shutdown), method, shardArrayIndex, indices, args);
    }

    public <T> IResultReceiver<T> broadcastToIndices(IResultPolicy<T> resultPolicy,
                                                  Method method,
                                                  int shardArrayIndex,
                                                  String[] indices,
                                                  Object... args) throws KattaException {
        if (indices == null) {
            indices = ALL_INDICES;
        }
        Map<String, List<String>> nodeShardsMap = getNode2ShardsMap(indices);
        if (nodeShardsMap.values().isEmpty()) {
            throw new KattaException("No shards for indices: "
                    + (indices != null ? Arrays.asList(indices).toString() : "null"));
        }
        return broadcastInternal(resultPolicy, method, shardArrayIndex, nodeShardsMap, args);
    }

    public <T> IResultReceiver<T> singlecast(long timeout,
                                          boolean shutdown,
                                          Method method,
                                          int shardArrayParamIndex,
                                          String shard,
                                          Object... args) throws KattaException {
        return singlecast(new ResultCompletePolicy<T>(timeout, shutdown), method, shardArrayParamIndex, shard, args);
    }

    public <T> IResultReceiver<T> singlecast(IResultPolicy<T> resultPolicy,
                                          Method method,
                                          int shardArrayParamIndex,
                                          String shard,
                                          Object... args) throws KattaException {
        List<String> shards = new ArrayList<String>();
        shards.add(shard);
        return broadcastToShards(resultPolicy, method, shardArrayParamIndex, shards, args);
    }

    public <T> IResultReceiver<T> broadcastToShards(long timeout,
                                                 boolean shutdown,
                                                 Method method,
                                                 int shardArrayParamIndex,
                                                 List<String> shards,
                                                 Object... args) throws KattaException {
        return broadcastToShards(new ResultCompletePolicy<T>(timeout, shutdown), method, shardArrayParamIndex, shards, args);
    }

    public <T> IResultReceiver<T> broadcastToShards(IResultPolicy<T> resultPolicy,
                                                 Method method,
                                                 int shardArrayParamIndex,
                                                 List<String> shards,
                                                 Object... args) throws KattaException {
        if (shards == null) {
            // If no shards specified, search all shards.
            shards = new ArrayList<String>();
            for (List<String> indexShards : indexToShards.values()) {
                shards.addAll(indexShards);
            }
        }
        final Map<String, List<String>> nodeShardsMap = this.selectionPolicy.createNode2ShardsMap(shards);
        if (nodeShardsMap.values().isEmpty()) {
            throw new KattaException("No shards selected: " + shards);
        }

        return broadcastInternal(resultPolicy, method, shardArrayParamIndex, nodeShardsMap, args);
    }


    /**
     * 这里调用会有问题,参数顺序不对
     * @param resultPolicy
     * @param method
     * @param shardArrayParamIndex
     * @param nodeShardsMap
     * @param args
     * @param <T>
     * @return
     */
    private <T> IResultReceiver<T> broadcastInternal(IResultPolicy<T> resultPolicy,
                                                  Method method,
                                                  int shardArrayParamIndex,
                                                  Map<String, List<String>> nodeShardsMap,
                                                  Object... args) {
        queryCount++;
        if (method == null || args == null) {
            throw new IllegalArgumentException("Null method or args!");
        }
        Class<?>[] types = method.getParameterTypes();
        if (args.length != types.length) {
            throw new IllegalArgumentException("Wrong number of args: found " + args.length + ", expected " + types.length
                    + "!");
        }
        for (int i = 0; i < args.length; i++) {
            if (args[i] != null) {
                Class<?> from = args[i].getClass();
                Class<?> to = types[i];
                if (!to.isAssignableFrom(from) && !(from.isPrimitive() || to.isPrimitive())) {
                    // Assume autoboxing will work.
                    throw new IllegalArgumentException("Incorrect argument type for param " + i + ": expected " + types[i] + "!");
                }
            }
        }
        if (shardArrayParamIndex > 0) {
            if (shardArrayParamIndex >= types.length) {
                throw new IllegalArgumentException("shardArrayParamIndex out of range!");
            }
            if (!(types[shardArrayParamIndex]).equals(String[].class)) {
                throw new IllegalArgumentException("shardArrayParamIndex parameter (" + shardArrayParamIndex
                        + ") is not of type String[]!");
            }
        }
        if (log.isTraceEnabled()) {
            for (Map.Entry<String, List<String>> e : indexToShards.entrySet()) {
                log.trace("indexToShards " + e.getKey() + " --> " + e.getValue().toString());
            }
            for (Map.Entry<String, List<String>> e : nodeShardsMap.entrySet()) {
                log.trace("broadcast using " + e.getKey() + " --> " + e.getValue().toString());
            }
            log.trace("selection policy = " + this.selectionPolicy);
        }

        //Make RPC calls to all nodes in parallel.
        long start = 0;
        if (log.isDebugEnabled()) {
            start = System.currentTimeMillis();
        }

        Map<String, List<String>> nodeShardMapCopy = new HashMap<String, List<String>>();
        Set<String> allShards = new HashSet<String>();


        for (Map.Entry<String, List<String>> e : nodeShardsMap.entrySet()) {
            nodeShardMapCopy.put(e.getKey(), new ArrayList<String>(e.getValue()));
            allShards.addAll(e.getValue());
        }


        nodeShardsMap = Collections.synchronizedMap(nodeShardMapCopy);
        nodeShardMapCopy = null;


        //这里多线程搜索
        Set<String> nodes = nodeShardsMap.keySet();
        WorkQueue<T> workQueue = new WorkQueue<T>(
                executor,
                proxyManager,
                allShards,
                nodes.size(),
                method,
                shardArrayParamIndex,
                args
        );
        for (String node : nodes) {
            workQueue.execute(node, nodeShardsMap, 1, maxTryCount);
        }

        IResultReceiver<T> results = workQueue.getResults(resultPolicy);

        if (log.isDebugEnabled()) {
            log.debug(String.format("broadcast(%s(%s), %s) took %d ms for %s",
                    method.getName(), args, nodeShardsMap,
                    (System.currentTimeMillis() - start),
                    results != null ? results : "null"));
        }
        return results;
    }

    // -------------------- Node management --------------------

    private Map<String, List<String>> getNode2ShardsMap(final String[] indexNames) throws KattaException {
        Collection<String> shardsToSearchIn = getShardsToSearchIn(indexNames);
        final Map<String, List<String>> nodeShardsMap = this.selectionPolicy.createNode2ShardsMap(shardsToSearchIn);
        return nodeShardsMap;
    }

    private Collection<String> getShardsToSearchIn(String[] indexNames) throws KattaException {
        Collection<String> allShards = new HashSet<String>();
        for (String index : indexNames) {
            if ("*".equals(index)) {
                for (Collection<String> shardsOfIndex : indexToShards.values()) {
                    allShards.addAll(shardsOfIndex);
                }
                break;
            }
            List<String> shardsForIndex = indexToShards.get(index);
            if (shardsForIndex != null) {
                allShards.addAll(shardsForIndex);
            } else {
                Pattern pattern = Pattern.compile(index);
                int matched = 0;
                for (String ind : indexToShards.keySet()) {
                    if (pattern.matcher(ind).matches()) {
                        allShards.addAll(indexToShards.get(ind));
                        matched++;
                    }
                }
                if (matched == 0) {
                    log.warn("No shards found for index name/pattern: " + index);
                }
            }
        }
        if (allShards.isEmpty()) {
            throw new KattaException("Index [pattern(s)] '" + Arrays.toString(indexNames)
                    + "' do not match to any deployed index: " + getIndices());
        }
        return allShards;
    }

    public double getQueryPerMinute() {
        double minutes = (System.currentTimeMillis() - STARTUP_TIME) / 60000.0;
        if (minutes > 0.0F) {
            return queryCount / minutes;
        }
        return 0.0F;
    }


    /**
     * 结束前关闭
     */
    public void close() {
        if (this.protocol != null) {
            this.protocol.unregisterComponent(this);
            this.protocol.disconnect();
            this.protocol = null;
            proxyManager.shutdown();
        }

        try{
            executor.shutdownNow();
        } catch (Exception e) {
            log.error("ex", e);
        }
    }

    @Override
    public void disconnect() {
        // nothing to do - only connection to zk dropped. Proxies might still be
        // available.
    }

    @Override
    public void reconnect() {
    }

    public List<String> getIndices() {
        return new ArrayList<String>(indexToShards.keySet());
    }


    public InteractionProtocol getProtocol() {
        return protocol;
    }
}
