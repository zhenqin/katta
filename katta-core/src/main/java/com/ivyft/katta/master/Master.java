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

import com.google.common.base.Preconditions;
import com.ivyft.katta.lib.lucene.FreeSocketPortFactory;
import com.ivyft.katta.operation.master.CheckIndicesOperation;
import com.ivyft.katta.operation.master.RemoveObsoleteShardsOperation;
import com.ivyft.katta.protocol.*;
import com.ivyft.katta.protocol.metadata.Version;
import com.ivyft.katta.protocol.upgrade.UpgradeAction;
import com.ivyft.katta.protocol.upgrade.UpgradeRegistry;
import com.ivyft.katta.util.KattaConfiguration;
import com.ivyft.katta.util.KattaException;
import com.ivyft.katta.util.MasterConfiguration;
import com.ivyft.katta.util.NetworkUtils;
import com.ivyft.katta.util.ZkConfiguration.PathDef;
import org.I0Itec.zkclient.ZkServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.UUID;



/**
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
public class Master implements ConnectedComponent {


    /**
     * 服务端需要处理其它节点的操作线程，都封装在这里
     */
    protected volatile OperatorThread operatorThread;


    /**
     * Master 配置信息
     */
    protected final MasterConfiguration masterConf;



    /**
     *
     * Master的名称。机器名+port
     */
    private String masterName;


    /**
     * Master 导入数据的 Port
     */
    protected int proxyBlckPort;


    /**
     * 该实例是ZOOkeeper的Server，会参与选举
     */
    private ZkServer zkServer;


    /**
     * 当前是否停机。 如果停机要停止所有Node
     */
    private boolean shutdownClient;


    /**
     * 封装的操作ZooKeeper的便捷操作
     */
    protected InteractionProtocol protocol;


    /**
     *
     * 服务端部署shard到Node一种策略
     */
    private IDeployPolicy deployPolicy;


    /**
     * 启动后safe模式的时间
     */
    private long safeModeMaxTime;


    /**
     * log
     */
    protected final static Logger LOG = LoggerFactory.getLogger(Master.class);


    /**
     *
     * @param interactionProtocol
     * @param zkServer
     * @throws KattaException
     */
    public Master(InteractionProtocol interactionProtocol, ZkServer zkServer) throws KattaException {
        this(interactionProtocol, false);
        this.zkServer = zkServer;
    }


    /**
     *
     * @param interactionProtocol
     * @param shutdownClient
     * @throws KattaException
     */
    public Master(InteractionProtocol interactionProtocol, boolean shutdownClient) throws KattaException {
        this(interactionProtocol, shutdownClient, new MasterConfiguration());
    }


    /**
     *
     * @param protocol
     * @param shutdownClient
     * @param masterConfiguration
     * @throws KattaException
     */
    public Master(InteractionProtocol protocol, boolean shutdownClient, MasterConfiguration masterConfiguration)
            throws KattaException {
        this.protocol = protocol;
        this.masterConf = masterConfiguration;
        this.masterName = NetworkUtils.getLocalhostName() + "_" + UUID.randomUUID().toString();
        this.shutdownClient = shutdownClient;

        this.proxyBlckPort = masterConfiguration.getInt(MasterConfiguration.PROXY_BLCK_PORT, 8440);


        String kattaMasterCode = masterName.substring(masterName.lastIndexOf("-") + 1);
        masterConfiguration.setProperty("katta.master.code", kattaMasterCode);

        LOG.info("katta.master.code: " + kattaMasterCode);

        //向ZooKeeper注册服务器
        protocol.registerComponent(this);

        //去的配置的IDeployPolicy实例
        final String deployPolicyClassName = masterConfiguration.getDeployPolicy();
        try {
            final Class<IDeployPolicy> policyClazz = (Class<IDeployPolicy>) Class.forName(deployPolicyClassName);
            //实例化deployPolicy对象
            this.deployPolicy = policyClazz.newInstance();
        } catch (final Exception e) {
            throw new KattaException("Unable to instantiate deploy policy", e);
        }

        this.safeModeMaxTime = masterConfiguration.getInt(MasterConfiguration.SAFE_MODE_MAX_TIME);
    }


    /**
     * 启动服务。
     */
    public synchronized void start() {
        Preconditions.checkState(!isShutdown(), "master was already shut-down");

        //TODO 无论是不是 Master, 都可以有导入数据的接口
        FreeSocketPortFactory socketPortFactory = new FreeSocketPortFactory();
        int step = masterConf.getInt(MasterConfiguration.PROXY_BLCK_PORT + ".step", 1);
        this.proxyBlckPort = socketPortFactory.getSocketPort(proxyBlckPort, step);
        LOG.info("start proxy blck server at: " + this.masterName + ":" + this.proxyBlckPort);

        KattaSocketServer server = new KattaSocketServer(KattaClientProtocol.class,
                new MasterStorageProtocol(this.masterConf, protocol));
        server.setHost(NetworkUtils.getLocalhostName());
        server.setPort(this.proxyBlckPort);
        server.setDaemon(false);
        try {
            server.init();
            protocol.registerMasters(this);
        } catch (Exception e) {
            LOG.error("", e);
        }

        //ZooKeeper选举
        becomePrimaryOrSecondaryMaster();
    }

    @Override
    public synchronized void reconnect() {
        disconnect();// just to be sure we do not open a 2nd operator thread
        becomePrimaryOrSecondaryMaster();
    }

    @Override
    public synchronized void disconnect() {
        if (isMaster()) {
            this.operatorThread.interrupt();
            try {
                this.operatorThread.join();
            } catch (InterruptedException e) {
                Thread.interrupted();
                // proceed
            }
            this.operatorThread = null;
        }
    }


    /**
     *
     * 初始化升级策略等，注册版本信息，启动服务，启动监听。
     */
    private synchronized void becomePrimaryOrSecondaryMaster() {
        if (isShutdown()) {
            return;
        }

        //发布当前为Master的主节点
        MasterQueue queue = this.protocol.publishMaster(this);

        //queue不是null,说明他成功的成为主节点了
        if (queue != null) {
            UpgradeAction upgradeAction =
                    UpgradeRegistry.findUpgradeAction(this.protocol,
                            Version.readFromJar());
            if (upgradeAction != null) {
                upgradeAction.upgrade(this.protocol);
            }

            //向ZooKeeper中写入版本等信息
            this.protocol.setVersion(Version.readFromJar());

            LOG.info(getMasterName() + " became master with " + queue.size() + " waiting master operations");


            //启动其他服务
            startNodeManagement();

            //实例化Master的上下文
            MasterContext masterContext = new MasterContext(
                    this.protocol,
                    this,
                    this.deployPolicy,
                    queue);

            //启动一个线程，监控所有变动
            this.operatorThread = new OperatorThread(masterContext,
                    this.safeModeMaxTime);
            this.operatorThread.start();
        }
    }


    /**
     * 返回Master是否是安全模式？ 安全模式表明当前Master不宜有大的改动
     * @return
     */
    public synchronized boolean isInSafeMode() {
        if (!isMaster()) {
            return true;
        }
        return this.operatorThread.isInSafeMode();
    }


    /**
     * 返回当前所有在线的Node
     * @return
     */
    public Collection<String> getConnectedNodes() {
        return this.protocol.getLiveNodes();
    }



    public synchronized MasterContext getContext() {
        if (!isMaster()) {
            return null;
        }
        return this.operatorThread.getContext();
    }



    public synchronized boolean isMaster() {
        return this.operatorThread != null;
    }

    private synchronized boolean isShutdown() {
        return this.protocol == null;
    }

    public String getMasterName() {
        return this.masterName;
    }


    /**
     * 升级为主节点
     */
    public void handleMasterDisappearedEvent() {
        becomePrimaryOrSecondaryMaster();
    }


    /**
     * 注册当前或者的节点，给所有活着的节点分发事件
     */
    private void startNodeManagement() {
        LOG.info("start managing nodes...");

        //监控/katta/live_nodes下节点的变化.这里保存真正活着的节点
        List<String> nodes = this.protocol.registerChildListener(this,
                PathDef.NODES_LIVE,
                new IAddRemoveListener() {

            @Override
            public void removed(String name) {
                synchronized (Master.this) {
                    if (!isInSafeMode()) {
                        Master.this.protocol.addMasterOperation(new CheckIndicesOperation());
                    }
                }
            }



            @Override
            public void added(String name) {
                synchronized (Master.this) {
                    if (!isMaster()) {
                        return;
                    }
                    Master.this.protocol.addMasterOperation(new RemoveObsoleteShardsOperation(name));
                    if (!isInSafeMode()) {
                        Master.this.protocol.addMasterOperation(new CheckIndicesOperation());
                    }
                }
            }
        });


        LOG.info("starting check index operator thread...");
        this.protocol.addMasterOperation(new CheckIndicesOperation());
        LOG.info("stop check index operator thread...");

        for (String node : nodes) {
            this.protocol.addMasterOperation(new RemoveObsoleteShardsOperation(node));
        }
        LOG.info("found following nodes connected: " + nodes);
    }


    /**
     * 关机。 停止Master服务。一般用于关机
     */
    public synchronized void shutdown() {
        LOG.info("stopping master...");
        if (this.protocol != null) {
            LOG.info("stop proxy blck server at: " + this.masterName + ":" + this.proxyBlckPort);

            LOG.info("unregistering from zookeeper.");
            this.protocol.unregisterComponent(this);
            if (isMaster()) {

                LOG.info("stopping master operator thread.");
                this.operatorThread.interrupt();
                try {
                    this.operatorThread.join();
                    this.operatorThread = null;
                } catch (final InterruptedException e1) {
                    // proceed
                }
            }
            if (this.shutdownClient) {
                LOG.info("master disconnect to zookeeper.");
                this.protocol.disconnect();
            }
            this.protocol = null;
            if (this.zkServer != null) {
                LOG.info("stopping embedded zookeeper server.");
                this.zkServer.shutdown();
                LOG.info("stopped embedded zookeeper server success.");
            }
        }

        LOG.info("stopped master.");
    }


    public int getProxyBlckPort() {
        return proxyBlckPort;
    }
}
