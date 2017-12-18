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
package com.ivyft.katta.protocol;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;

import com.google.common.collect.Sets;
import com.ivyft.katta.master.Master;
import com.ivyft.katta.node.Node;
import com.ivyft.katta.node.monitor.MetricsRecord;
import com.ivyft.katta.operation.OperationId;
import com.ivyft.katta.operation.master.MasterOperation;
import com.ivyft.katta.operation.node.NodeOperation;
import com.ivyft.katta.operation.node.OperationResult;
import com.ivyft.katta.protocol.metadata.*;
import com.ivyft.katta.util.*;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.util.ZkPathUtil;
import org.I0Itec.zkclient.util.ZkPathUtil.PathFilter;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;


import static com.ivyft.katta.util.ZkConfiguration.*;

/**
 * <p>
 *   Abstracts the interaction between master and nodes via zookeeper files and
 * folders.
 * <p/>
 * <p/>
 * For inspecting and understanding the zookeeper structure see
 * {@link #explainStructure()} and {@link #showStructure(boolean)}}.
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
public class InteractionProtocol {


    /**
     * 当前链接与否
     */
    protected volatile boolean _connected = true;

    /**
     * 连接ZooKeeper的客户端
     */
    protected ZkClient _zkClient;

    /**
     * Node的配置信息
     */
    protected ZkConfiguration zkConf;


    /**
     * we govern the various listener and ephemerals to remove burden from
     * listener-users to unregister/delete them
     *
     * 该对象保存这所有在ZooKeeper注册的Node
     */
    protected ConcurrentOne2ManyListMap<ConnectedComponent, ListenerAdapter>
            _zkListenerByComponent = new ConcurrentOne2ManyListMap<ConnectedComponent, ListenerAdapter>();


    /**
     *
     *
     */
    private SetMultimap<ConnectedComponent, String> _zkEphemeralPublishesByComponent = HashMultimap.create();


    /**
     */
    private IZkStateListener stateListener = new IZkStateListener() {

        @Override
        public void handleStateChanged(KeeperState state) throws Exception {
            Set<ConnectedComponent> components = new HashSet<ConnectedComponent>(_zkListenerByComponent.keySet());
            switch (state) {
                case Disconnected:

                case Expired:
                    if (_connected) {
                        // disconnected & expired can come after each other
                        //选举失败，这个主节点应该退出
                        for (ConnectedComponent component : components) {
                            component.disconnect();
                        }
                        _connected = false;
                    }
                    LOG.info("master disconnect");
                    break;
                case SyncConnected:
                    //选举成功， 这个主节点重试链接
                    _connected = true;
                    for (ConnectedComponent kattaComponent : components) {
                        kattaComponent.reconnect();
                    }
                    LOG.info("master reconnect");
                    break;
                default:
                    throw new IllegalStateException("state " + state + " not handled");
            }
        }

        @Override
        public void handleNewSession() throws Exception {
            // should be covered by handleStateChanged()
            LOG.info("new Session:" + DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
        }

        public void handleSessionEstablishmentError(Throwable error) throws Exception {
            LOG.info("Establishment Session:" + DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
        }
    };


    /**
     * log
     */
    protected final static Logger LOG = LoggerFactory.getLogger("InteractionProtocol");


    /**
     *
     * @param zkClient
     * @param zkConfiguration
     */
    public InteractionProtocol(ZkClient zkClient, ZkConfiguration zkConfiguration) {
        _zkClient = zkClient;
        this.zkConf = zkConfiguration;
        LOG.debug("Using ZK root path: " + this.zkConf.getZkRootPath());
        new DefaultNameSpaceImpl(this.zkConf).createDefaultNameSpace(_zkClient);
    }


    /**
     *
     * 注册当前Master节点
     * @param connectedComponent
     */
    public void registerComponent(ConnectedComponent connectedComponent) {
        if (_zkListenerByComponent.size() == 0) {
            _zkClient.subscribeStateChanges(this.stateListener);
        }
        _zkListenerByComponent.add(connectedComponent);
    }


    /**
     * 取消注册的Node, 在 5S 钟之内
     * @param component 组件
     */
    public void unregisterComponent(ConnectedComponent component) {
        List<ListenerAdapter> listeners = _zkListenerByComponent.removeKey(component);
        for (ListenerAdapter listener : listeners) {
            if (listener instanceof IZkChildListener) {
                _zkClient.unsubscribeChildChanges(listener.getPath(), (IZkChildListener) listener);
            } else if (listener instanceof IZkDataListener) {
                _zkClient.unsubscribeDataChanges(listener.getPath(), (IZkDataListener) listener);
            } else {
                throw new IllegalStateException("could not handle lister of type " + listener.getClass().getName());
            }
        }
        // we deleting the ephemeral's since this is the fastest and the safest

        // way, but if this does not work, it shouldn't be too bad
        Collection<String> zkPublishes = _zkEphemeralPublishesByComponent.removeAll(component);
        for (String zkPath : zkPublishes) {
            _zkClient.delete(zkPath);
        }

        if (_zkListenerByComponent.size() == 0) {
            _zkClient.unsubscribeStateChanges(this.stateListener);
        }
        LOG.info("unregistering component " + component + ": " + _zkListenerByComponent);
    }


    /**
     * 停机断开zookeeper的链接
     */
    public void disconnect() {
        if (_zkListenerByComponent.size() > 0) {
            LOG.warn("following components still connected:" + _zkListenerByComponent.keySet());
        }
        if (_zkEphemeralPublishesByComponent.size() > 0) {
            LOG.warn("following ephemeral still exists:" + _zkEphemeralPublishesByComponent.keySet());
        }
        _connected = false;
        _zkClient.close();
    }



    public int getRegisteredComponentCount() {
        return _zkListenerByComponent.size();
    }



    public int getRegisteredListenerCount() {
        int count = 0;
        Collection<List<ListenerAdapter>> values = _zkListenerByComponent.asMap().values();
        for (List<ListenerAdapter> list : values) {
            count += list.size();
        }
        return count;
    }



    public List<String> registerChildListener(ConnectedComponent component, ZkConfiguration.PathDef pathDef, IAddRemoveListener listener) {
        return registerAddRemoveListener(component, listener, this.zkConf.getZkPath(pathDef));
    }

    public List<String> registerChildListener(ConnectedComponent component, ZkConfiguration.PathDef pathDef, String childName,
                                              IAddRemoveListener listener) {
        return registerAddRemoveListener(component, listener, this.zkConf.getZkPath(pathDef, childName));
    }

    public void unregisterChildListener(ConnectedComponent component, ZkConfiguration.PathDef pathDef) {
        unregisterAddRemoveListener(component, this.zkConf.getZkPath(pathDef));
    }

    public void unregisterChildListener(ConnectedComponent component, ZkConfiguration.PathDef pathDef, String childName) {
        unregisterAddRemoveListener(component, this.zkConf.getZkPath(pathDef, childName));
    }

    public void registerDataListener(ConnectedComponent component, ZkConfiguration.PathDef pathDef, String childName,
                                     IZkDataListener listener) {
        registerDataListener(component, listener, this.zkConf.getZkPath(pathDef, childName));
    }

    public void unregisterDataChanges(ConnectedComponent component, ZkConfiguration.PathDef pathDef, String childName) {
        unregisterDataListener(component, this.zkConf.getZkPath(pathDef, childName));
    }

    public void unregisterDataChanges(ConnectedComponent component, String dataPath) {
        unregisterDataListener(component, dataPath);
    }

    private void registerDataListener(ConnectedComponent component, IZkDataListener dataListener, String zkPath) {
        synchronized (component) {
            ZkDataListenerAdapter listenerAdapter = new ZkDataListenerAdapter(dataListener, zkPath);
            _zkClient.subscribeDataChanges(zkPath, listenerAdapter);
            _zkListenerByComponent.add(component, listenerAdapter);
        }
    }

    private void unregisterDataListener(ConnectedComponent component, String zkPath) {
        synchronized (component) {
            ZkDataListenerAdapter listenerAdapter = getComponentListener(component, ZkDataListenerAdapter.class, zkPath);
            _zkClient.unsubscribeDataChanges(zkPath, listenerAdapter);
            _zkListenerByComponent.removeValue(component, listenerAdapter);
        }
    }

    private List<String> registerAddRemoveListener(ConnectedComponent component,
                                                   IAddRemoveListener listener,
                                                   String zkPath) {
        synchronized (component) {
            AddRemoveListenerAdapter listenerAdapter = new AddRemoveListenerAdapter(zkPath, listener);
            synchronized (listenerAdapter) {
                List<String> childs = _zkClient.subscribeChildChanges(zkPath, listenerAdapter);
                listenerAdapter.setCachedChilds(childs);
            }
            _zkListenerByComponent.add(component, listenerAdapter);
            return listenerAdapter.getCachedChilds();
        }
    }

    private void unregisterAddRemoveListener(ConnectedComponent component, String zkPath) {
        synchronized (component) {
            AddRemoveListenerAdapter listenerAdapter = getComponentListener(component, AddRemoveListenerAdapter.class, zkPath);
            _zkClient.unsubscribeChildChanges(zkPath, listenerAdapter);
            _zkListenerByComponent.removeValue(component, listenerAdapter);
        }
    }

    private <T extends ListenerAdapter> T getComponentListener(ConnectedComponent component,
                                                               Class<T> listenerClass, String zkPath) {
        for (ListenerAdapter pathListener : _zkListenerByComponent.getValues(component)) {
            if (listenerClass.isAssignableFrom(pathListener.getClass()) && pathListener.getPath().equals(zkPath)) {
                return (T) pathListener;
            }
        }
        throw new IllegalStateException("no listener adapter for component " + component + " and path " + zkPath
                + " found: " + _zkListenerByComponent);
    }


    /**
     * 获得所有注册过的Node
     * @return 返回注册过的Node, 当前活着的+已经down掉的.
     */
    public List<String> getKnownNodes() {
        return _zkClient.getChildren(this.zkConf.getZkPath(PathDef.NODES_METADATA));
    }


    /**
     * 获得所有活着的Node
     * @return 返回当前活着的Node
     */
    public List<String> getLiveNodes() {
        return _zkClient.getChildren(this.zkConf.getZkPath(ZkConfiguration.PathDef.NODES_LIVE));
    }


    /**
     * 获得所有分片
     * @return 返回所有分片的集合
     */
    public List<String> getIndices() {
        return _zkClient.getChildren(this.zkConf.getZkPath(ZkConfiguration.PathDef.INDICES_METADATA));
    }


    /**
     * 查询当前的Master
     * @return 返回当前的Master
     */
    public MasterMetaData getMasterMD() {
        return (MasterMetaData) readZkData(this.zkConf.getZkPath(ZkConfiguration.PathDef.MASTER));
    }



    /**
     * 获得Node代表的注册的Node
     * @param node 返回注册过的Node的信息
     * @return 返回注册过的Node的信息, 可能活着,也可能down掉的.
     */
    public NodeMetaData getNodeMD(String node) {
        return (NodeMetaData) readZkData(this.zkConf.getZkPath(ZkConfiguration.PathDef.NODES_METADATA, node));
    }


    /**
     * 读ZKzkPath节点的数据
     * @param zkPath
     * @return
     */
    private Serializable readZkData(String zkPath) {
        Serializable data = null;
        if (_zkClient.exists(zkPath)) {
            data = _zkClient.readData(zkPath);
        }
        return data;
    }


    /**
     * 获得指定node的shard集合
     *
     * @param nodeName node名称
     *
     * @return 返回nodeName添加的shard集合。nodeName没有shard返回size=0的集合
     */
    public Collection<String> getNodeShards(String nodeName) {
        Set<String> shards = new HashSet<String>();

        //获得所有的shard
        List<String> shardNames = _zkClient.getChildren(this.zkConf.getZkPath(PathDef.SHARD_TO_NODES));

        //遍历shard下的node。每个shard节点下存放着包涵他的node
        for (String shardName : shardNames) {
            List<String> nodeNames = _zkClient.getChildren(this.zkConf.getZkPath(PathDef.SHARD_TO_NODES, shardName));

            //验证是否有当前节点nodeName
            if (nodeNames.contains(nodeName)) {
                shards.add(shardName);
            }
        }
        return shards;
    }



    public int getShardReplication(String shard) {
        return _zkClient.countChildren(this.zkConf.getZkPath(ZkConfiguration.PathDef.SHARD_TO_NODES, shard));
    }



    public long getShardAnnounceTime(String node, String shard) {
        return _zkClient.getCreationTime(this.zkConf.getZkPath(ZkConfiguration.PathDef.SHARD_TO_NODES, shard, node));
    }


    /**
     * 检验每个shard被几个Node加载
     * @param shardNames
     * @return
     */
    public Map<String, List<String>> getShard2NodesMap(Collection<String> shardNames) {
        Map<String, List<String>> shard2NodeNames = new HashMap<String, List<String>>();
        for (String shard : shardNames) {
            String shard2NodeRootPath = this.zkConf.getZkPath(ZkConfiguration.PathDef.SHARD_TO_NODES, shard);
            if (_zkClient.exists(shard2NodeRootPath)) {
                shard2NodeNames.put(shard, _zkClient.getChildren(shard2NodeRootPath));
            } else {
                shard2NodeNames.put(shard, Collections.<String>emptyList());
            }
        }
        return shard2NodeNames;
    }


    /**
     * 获得某个Shard部署在的Node集合
     * @param shard 一个shard
     * @return 返回被加载的Shard的Node集合
     */
    public List<String> getShardNodes(String shard) {
        String shard2NodeRootPath = this.zkConf.getZkPath(ZkConfiguration.PathDef.SHARD_TO_NODES, shard);
        if (!_zkClient.exists(shard2NodeRootPath)) {
            return Collections.<String>emptyList();
        }
        return _zkClient.getChildren(shard2NodeRootPath);
    }


    /**
     * 返回所有注册的Shard
     * @return 返回所有注册的Shard
     */
    public List<String> getShard2NodeShards() {
        return _zkClient.getChildren(this.zkConf.getZkPath(ZkConfiguration.PathDef.SHARD_TO_NODES));
    }



    public ReplicationReport getReplicationReport(IndexMetaData indexMD) {
        int desiredReplicationCount = indexMD.getReplicationLevel();
        int minimalShardReplicationCount = indexMD.getReplicationLevel();
        int maximaShardReplicationCount = 0;

        Map<String, Integer> replicationCountByShardMap = new HashMap<String, Integer>();
        Set<Shard> shards = indexMD.getShards();
        for (Shard shard : shards) {
            int servingNodesCount = getShardNodes(shard.getName()).size();
            replicationCountByShardMap.put(shard.getName(), servingNodesCount);
            if (servingNodesCount < minimalShardReplicationCount) {
                minimalShardReplicationCount = servingNodesCount;
            }
            if (servingNodesCount > maximaShardReplicationCount) {
                maximaShardReplicationCount = servingNodesCount;
            }
        }
        return new ReplicationReport(replicationCountByShardMap, desiredReplicationCount, minimalShardReplicationCount,
                maximaShardReplicationCount);
    }


    /**
     * 向 Zookeeper 的 /katta/masters/ 下写入 Master 的启动端口号信息
     * @param master
     */
    public void registerMasters(final Master master) {
        String masterName = master.getMasterName();
        int proxyBlckPort = master.getProxyBlckPort();

        String masters = this.zkConf.getZkPath(PathDef.MASTERS);
        if(!_zkClient.exists(masters)){
            _zkClient.createPersistent(masters);
            LOG.info("create masters: " + masters);
        }

        String zkMasterPath = this.zkConf.getZkPath(PathDef.MASTERS, masterName + ":" + proxyBlckPort);

        createEphemeral(master, zkMasterPath, masterName + ":" + proxyBlckPort);
        LOG.info("create ephemeral path: " + zkMasterPath);

    }


    /**
     * 获得 Master OR secondary Master 的 port
     * @return
     */
    public List<String> getMasters() {
        String zkMasterPath = this.zkConf.getZkPath(PathDef.MASTERS);
        List<String> children = _zkClient.getChildren(zkMasterPath);
        return Lists.newArrayList(children);
    }



    /**
     *
     * 注册当前Master到ZooKeeper.
     * @param master master
     * @return 返回注册事件
     */
    public MasterQueue publishMaster(final Master master) {
        String masterName = master.getMasterName();

        String zkMasterPath = this.zkConf.getZkPath(PathDef.MASTER);

        //清除原来残留的信息
        cleanupOldMasterData(masterName, zkMasterPath);

        LOG.info(masterName + " to become a master.");
        boolean isMaster;
        try {

            //创建Master的节点. 当前是Master肯定能创建成功
            MasterMetaData masterMetaData = new MasterMetaData(masterName,
                    master.getProxyBlckPort(),
                    System.currentTimeMillis());

            createEphemeral(master, zkMasterPath, masterMetaData);

            //走到这里说明这个Master成功的成为主节点
            isMaster = true;
            LOG.info(masterName + " started as master");
        } catch (ZkNodeExistsException e) {
            registerDataListener(master, new IZkDataListener() {
                @Override
                public void handleDataDeleted(String dataPath) throws KattaException {

                    //当上一个Master挂掉, 这个升级为主节点
                    master.handleMasterDisappearedEvent();
                }

                @Override
                public void handleDataChange(String dataPath, Object data) throws Exception {
                    // do nothing
                }
            }, zkMasterPath);
            isMaster = false;
            LOG.info(masterName + " started as secondary master");
        }

        //构造当前注册事件,是不是Master?
        MasterQueue queue = null;
        if (isMaster) {
            LOG.info("master '" + master.getMasterName() + "' published");

            //Master在ZK中分发事件的地址
            String queuePath = this.zkConf.getZkPath(ZkConfiguration.PathDef.MASTER_QUEUE);
            if (!_zkClient.exists(queuePath)) {
                _zkClient.createPersistent(queuePath);
            }

            //成功了, 就是Master
            queue = new MasterQueue(_zkClient, queuePath);
        } else {

            //注册不成功就是个secondary
            LOG.info("secondary master '" + master.getMasterName() + "' registered");
        }

        return queue;
    }

    private void cleanupOldMasterData(String masterName, String zkMasterPath) {
        if (_zkClient.exists(zkMasterPath)) {
            MasterMetaData existingMaster = _zkClient.readData(zkMasterPath);
            if (existingMaster.getMasterName().equals(masterName)) {
                LOG.warn("detected old master entry pointing to this host - deleting it..");
                _zkClient.delete(zkMasterPath);
            }
        }
    }

    public NodeQueue publishNode(Node node, NodeMetaData nodeMetaData) {
        LOG.info("publishing node '" + node.getName() + "' ...");

        //在Livenode下写入临时的节点信息
        String nodePath = this.zkConf.getZkPath(ZkConfiguration.PathDef.NODES_LIVE, node.getName());

        //在metadata中写入永久的节点信息
        String nodeMetadataPath = this.zkConf.getZkPath(ZkConfiguration.PathDef.NODES_METADATA, node.getName());

        if (_zkClient.exists(nodeMetadataPath)) {
            _zkClient.writeData(nodeMetadataPath, nodeMetaData);
        } else {
            _zkClient.createPersistent(nodeMetadataPath, nodeMetaData);
        }

        // create queue for incoming node operations
        String queuePath = this.zkConf.getZkPath(ZkConfiguration.PathDef.NODE_QUEUE, node.getName());
        if (_zkClient.exists(queuePath)) {
            _zkClient.deleteRecursive(queuePath);
        }

        NodeQueue nodeQueue = new NodeQueue(_zkClient, queuePath);

        // mark the node as connected
        if (_zkClient.exists(nodePath)) {
            LOG.warn("Old node ephemeral '" + nodePath + "' detected, deleting it...");
            _zkClient.delete(nodePath);
        }

        //创建临时节点。并把节点注册信息保存到_zkEphemeralPublishesByComponent
        createEphemeral(node, nodePath, null);
        return nodeQueue;
    }



    public void createIndex(NewIndexMetaData newIndexMetaData) {
        String newIndex = this.zkConf.getZkPath(PathDef.NEW_INDICES);
        if(!_zkClient.exists(newIndex)){
            _zkClient.createPersistent(newIndex);
            LOG.info("create new indices path: " + newIndex);
        }

        String newIndexPath = this.zkConf.getZkPath(PathDef.NEW_INDICES, newIndexMetaData.getName());

        //(master, zkMasterPath, masterName + ":" + proxyBlckPort);
        _zkClient.createPersistent(newIndexPath, newIndexMetaData);
        LOG.info("create path: " + newIndexPath);
    }




    public List<String> getNewIndexs() {
        String newIndex = this.zkConf.getZkPath(PathDef.NEW_INDICES);
        if(!_zkClient.exists(newIndex)){
            _zkClient.createPersistent(newIndex);
            LOG.info("create new indices path: " + newIndex);

            return new ArrayList<String>(0);
        }

        List<String> children = _zkClient.getChildren(newIndex);
        if(children == null) {
            return new ArrayList<String>(0);
        }
        return children;
    }


    public void getNewCommit(String commitId, CommitShards commitShards, Map<String, Map<String, Map<String, Object>>> nodeCommitMeta) {
        String commitsPath = this.zkConf.getZkPath(PathDef.COMMIT);
        if(!_zkClient.exists(commitsPath)){
            _zkClient.createPersistent(commitsPath);
            LOG.info("create commit path: " + commitsPath);
        }
        String path = this.zkConf.getZkPath(PathDef.COMMIT, commitId);
        _zkClient.createPersistent(path, nodeCommitMeta);
        LOG.info("create commitId path: " + path);
    }



    public Map<String, Object> getCommitData(String commitId) {
        String commitsPath = this.zkConf.getZkPath(PathDef.COMMIT);
        if(!_zkClient.exists(commitsPath)){
            _zkClient.createPersistent(commitsPath);
            LOG.info("create commit path: " + commitsPath);
        }

        String path = this.zkConf.getZkPath(PathDef.COMMIT, commitId);
        if(_zkClient.exists(path)) {
            return _zkClient.readData(path);
        }
        return null;
    }



    public void deleteZkPath(String commitId) {
        String path = this.zkConf.getZkPath(PathDef.COMMIT, commitId);
        if(_zkClient.exists(path)) {
            _zkClient.delete(path);
            LOG.info("delete commitId path: " + path);
        }
    }


    public NewIndexMetaData getNewIndex(String newIndexName) {
        String newIndex = this.zkConf.getZkPath(PathDef.NEW_INDICES);
        if(!_zkClient.exists(newIndex)){
            _zkClient.createPersistent(newIndex);
            LOG.info("create new indices path: " + newIndex);

            return null;
        }

        String newIndexPath = this.zkConf.getZkPath(PathDef.NEW_INDICES, newIndexName);

        if(!_zkClient.exists(newIndexPath)){
            return null;
        }
        NewIndexMetaData newIndexMetaData = _zkClient.readData(newIndexPath);
        return newIndexMetaData;
    }


    /**
     * 部署Index， 把IndexName写入INDICES
     * @param indexMD 索引信息
     */
    public void publishIndex(IndexMetaData indexMD) {
        _zkClient.createPersistent(
                this.zkConf.getZkPath(PathDef.INDICES_METADATA,
                        indexMD.getName()), indexMD);
    }


    /**
     * 卸载一个Index，会删除ZooKeeper中写入的信息。
     * @param indexName Index Name
     */
    public void unpublishIndex(String indexName) {
        IndexMetaData indexMD = getIndexMD(indexName);
        _zkClient.delete(this.zkConf.getZkPath(PathDef.INDICES_METADATA, indexName));
        for (Shard shard : indexMD.getShards()) {
            _zkClient.deleteRecursive(this.zkConf.getZkPath(PathDef.SHARD_TO_NODES, shard.getName()));
        }
    }


    /**
     * 返回一个Index的元信息，配置信息。
     * @param index
     * @return
     */
    public IndexMetaData getIndexMD(String index) {
        return (IndexMetaData) readZkData(this.zkConf.getZkPath(ZkConfiguration.PathDef.INDICES_METADATA, index));
    }


    /**
     * 更新Index 元信息
     * @param indexMD
     */
    public void updateIndexMD(IndexMetaData indexMD) {
        _zkClient.writeData(this.zkConf.getZkPath(ZkConfiguration.PathDef.INDICES_METADATA, indexMD.getName()), indexMD);
    }


    /**
     * 给Node部署一个Shard
     * @param node 节点
     * @param shardName shardName
     */
    public void publishShard(Node node, String shardName) {
        // announce that this node serves this shard now...
        String shard2NodePath = this.zkConf.getZkPath(ZkConfiguration.PathDef.SHARD_TO_NODES, shardName, node.getName());
        if (_zkClient.exists(shard2NodePath)) {
            //delete path: /katta/shard-to-nodes/test#index1/nodename:port
            LOG.warn("detected old shard-to-node entry " + shard2NodePath + " - deleting it..");
            _zkClient.delete(shard2NodePath);
        }

        //在shard-to-nodes下创建当前shard节点，表示注册一个shard
        //create path: /katta/shard-to-nodes/test#index1
        _zkClient.createPersistent(this.zkConf.getZkPath(PathDef.SHARD_TO_NODES, shardName), true);

        //在shard下创建node节点。出发node加载shard
        //create path: /katta/shard-to-nodes/test#index1/nodeName:port
        createEphemeral(node, shard2NodePath, null);
    }


    /**
     * 从node上卸载一个shard
     * @param node node名称
     * @param shard shard名称
     */
    public void unpublishShard(Node node, String shard) {
        String shard2NodePath = this.zkConf.getZkPath(PathDef.SHARD_TO_NODES, shard, node.getName());
        if (_zkClient.exists(shard2NodePath)) {
            _zkClient.delete(shard2NodePath);
            LOG.warn("unpublish shard, detected shard-to-node entry, shard: " + shard +
                    " node: " + node.getName() + " path:" + shard2NodePath);
        }

        String shardPath = this.zkConf.getZkPath(PathDef.SHARD_TO_NODES, shard);
        int shardCount = 0;
        try {
            shardCount = _zkClient.countChildren(shardPath);
        } catch (Exception e) {
            shardCount = 0;
        }

        if (_zkClient.exists(shardPath) && shardCount == 0) {
            _zkClient.delete(shardPath);
            LOG.warn("unpublish shard, detected shard-to-node's shard entry: " + shardPath);
        }
        _zkEphemeralPublishesByComponent.remove(node, shard2NodePath);
    }


    /**
     * 把日志信息写入到监控系统
     * @param nodeName
     * @param metricsRecord
     */
    public void setMetric(String nodeName, MetricsRecord metricsRecord) {
        String metricsPath = this.zkConf.getZkPath(PathDef.NODE_METRICS, nodeName);
        try {
            _zkClient.writeData(metricsPath, metricsRecord);
        } catch (ZkNoNodeException e) {
            _zkClient.createEphemeral(metricsPath, new MetricsRecord(nodeName));
        } catch (Exception e) {
            // this only happens if zk is down
            LOG.debug("Can't write to zk", e);
        }
    }


    /**
     * 取得最新日志
     * @param nodeName
     * @return
     */
    public MetricsRecord getMetric(String nodeName) {
        return (MetricsRecord) readZkData(this.zkConf.getZkPath(PathDef.NODE_METRICS, nodeName));
    }


    /**
     * @param component Zookeeper 访问组件
     * @param path 路径
     * @param content 内容
     */
    private void createEphemeral(ConnectedComponent component, String path, Serializable content) {
        _zkClient.createEphemeral(path, content);

        //放入_zkEphemeralPublishesByComponent
        _zkEphemeralPublishesByComponent.put(component, path);
        //LOG.info(_zkEphemeralPublishesByComponent.toString());
    }


    /**
     * Master分发一个事件给Node
     * @param operation
     */
    public void addMasterOperation(MasterOperation operation) {
        //ZK目录: /katta/work/master-queue
        String queuePath = this.zkConf.getZkPath(PathDef.MASTER_QUEUE);

        new MasterQueue(_zkClient, queuePath).add(operation);
    }



    public OperationId addNodeOperation(String nodeName, NodeOperation nodeOperation) {
        String elementName = getNodeQueue(nodeName).add(nodeOperation);
        return new OperationId(nodeName, elementName);
    }

    public OperationResult getNodeOperationResult(OperationId operationId, boolean remove) {
        return (OperationResult) getNodeQueue(operationId.getNodeName()).getResult(operationId.getElementName(), remove);
    }

    public boolean isNodeOperationQueued(OperationId operationId) {
        return getNodeQueue(operationId.getNodeName()).containsElement(operationId.getElementName());
    }

    public void registerNodeOperationListener(ConnectedComponent component, OperationId operationId,
                                              IZkDataListener dataListener) {
        String elementPath = getNodeQueue(operationId.getNodeName()).getElementPath(operationId.getElementName());
        registerDataListener(component, dataListener, elementPath);
    }

    private NodeQueue getNodeQueue(String nodeName) {
        //DIR: /katta/nodes/node-queues
        String queuePath = this.zkConf.getZkPath(PathDef.NODE_QUEUE, nodeName);
        return new NodeQueue(_zkClient, queuePath);
    }

    public boolean indexExists(String indexName) {
        return _zkClient.exists(this.zkConf.getZkPath(PathDef.INDICES_METADATA, indexName));
    }

    public void showStructure(boolean all) {
        String string;
        if (all) {
            string = ZkPathUtil.toString(_zkClient, this.zkConf.getZkRootPath(), PathFilter.ALL);
        } else {
            final Set<String> nonViPathes = new HashSet<String>();
            for (PathDef pathDef : PathDef.values()) {
                if (!pathDef.isVip()) {
                    nonViPathes.add(this.zkConf.getZkPath(pathDef));
                }
            }
            string = ZkPathUtil.toString(_zkClient, this.zkConf.getZkRootPath(), new PathFilter() {
                @Override
                public boolean showChilds(String path) {
                    return !nonViPathes.contains(path);
                }
            });
        }
        System.out.println(string);
    }

    public void explainStructure() {
        for (PathDef pathDef : PathDef.values()) {
            String zkPath = this.zkConf.getZkPath(pathDef);
            System.out.println(StringUtil.fillWithWhiteSpace(zkPath, 40) + "\t" + pathDef.getDescription());
        }
    }


    /**
     * 在ZooKeeper中写入Katta的版本信息
     * @param version
     */
    public void setVersion(Version version) {
        String zkPath = this.zkConf.getZkPath(PathDef.VERSION);
        try {
            _zkClient.writeData(zkPath, version);
        } catch (ZkNoNodeException e) {
            _zkClient.createPersistent(zkPath, version);
        }
    }


    /**
     *
     * @return
     */
    public Version getVersion() {
        return _zkClient.readData(this.zkConf.getZkPath(PathDef.VERSION), true);
    }

    public void setFlag(String name) {
        _zkClient.createEphemeral(this.zkConf.getZkPath(PathDef.FLAGS, name));
    }

    public boolean flagExists(String name) {
        return _zkClient.exists(this.zkConf.getZkPath(PathDef.FLAGS, name));
    }

    public void removeFlag(String name) {
        _zkClient.delete(this.zkConf.getZkPath(PathDef.FLAGS, name));
    }

    public ZkConfiguration getZkConfiguration() {
        return this.zkConf;
    }

    public ZkClient getZkClient() {
        return _zkClient;
    }

}
