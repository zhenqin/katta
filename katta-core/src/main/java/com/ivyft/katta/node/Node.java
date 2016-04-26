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
package com.ivyft.katta.node;

import com.ivyft.katta.lib.lucene.FreeSocketPortFactory;
import com.ivyft.katta.lib.lucene.ILuceneServer;
import com.ivyft.katta.lib.lucene.SocketPortFactory;
import com.ivyft.katta.node.monitor.IMonitor;
import com.ivyft.katta.operation.node.NodeOperationProcessor;
import com.ivyft.katta.operation.node.ShardRedeployOperation;
import com.ivyft.katta.protocol.ConnectedComponent;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.NodeQueue;
import com.ivyft.katta.protocol.metadata.NodeMetaData;
import com.ivyft.katta.util.HadoopUtil;
import com.ivyft.katta.util.NetworkUtils;
import com.ivyft.katta.util.NodeConfiguration;
import com.ivyft.katta.util.ThrottledInputStream;
import org.I0Itec.zkclient.ExceptionUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


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
public class Node implements ConnectedComponent {


    /**
     * Node的配置
     */
    private final NodeConfiguration _nodeConf;


    /**
     * 封装了所有的ZooKeeper监控操作
     */
    protected InteractionProtocol _protocol;


    /**
     * 每个Node持有的shard的操作。 如移除shard，添加shard。 返回shard信息等
     */
    private final IContentServer _contentServer;


    /**
     * Node需要的上下文信息，保存重要的变量，操作等
     */
    protected NodeContext _context;

    /**
     * 是一个机器名+RPCPort的字符串。标识当前的机器
     */
    protected String _nodeName;


    /**
     * Hadoop RPC的提供服务方
     */
    private Server _rpcServer;


    /**
     * 监控节点
     */
    private IMonitor _monitor;


    /**
     * Node工作的线程
     */
    private Thread _nodeOperatorThread;


    /**
     * 当前的节点停止OR正在运行
     */
    private boolean _stopped;


    /**
     * log
     */
    protected final static Logger LOG = LoggerFactory.getLogger(Node.class);


    /**
     * 构造方法。 这个方法是没用的
     * @param protocol
     * @param server
     */
    public Node(InteractionProtocol protocol, IContentServer server) {
        this(protocol, new NodeConfiguration(), server);
    }


    /**
     * 构造方法。 调用放见
     *
     * @param protocol 提供操作ZooKeeper的封装
     * @param configuration Node的配置
     * @param contentServer LuceneServer的实例
     */
    public Node(InteractionProtocol protocol,
                final NodeConfiguration configuration,
                IContentServer contentServer) {
        _protocol = protocol;
        _contentServer = contentServer;
        if (contentServer == null) {
            throw new IllegalArgumentException("Null server passed to Node()");
        }
        _nodeConf = configuration;
    }


    /**
     * 启动Node
     */
    public void start() {
        if (_stopped) {
            throw new IllegalStateException("Node cannot be started again after it was shutdown.");
        }
        LOG.info("starting rpc server with  server class = " + _contentServer.getClass().getCanonicalName());
        String hostName = NetworkUtils.getLocalhostName();

        int port = _nodeConf.getInt(NodeConfiguration.NODE_SERVER_PORT_START, _nodeConf.getStartPort());
        SocketPortFactory factory = new FreeSocketPortFactory();
        int step = _nodeConf.getInt(NodeConfiguration.NODE_SERVER_PORT_START + ".step", 1);
        port = factory.getSocketPort(port, step);

        //启动Hadoop 的RPC
        _rpcServer = startRPCServer(
                hostName,
                port,
                _contentServer,
                _nodeConf.getRpcHandlerCount());

        //把新端口号写到 NODE
        _nodeConf.setProperty(NodeConfiguration.NODE_SERVER_PORT_START, port);

        _nodeName = hostName + ":" + _rpcServer.getListenerAddress().getPort();

        _contentServer.init(_nodeName, _nodeConf);

        // we add hostName and port to the shardFolder to allow multiple nodes per
        // server with the same configuration
        File shardsFolder = new File(_nodeConf.getShardFolder(), _nodeName.replaceAll(":", "_"));
        LOG.info("local shard folder: " + shardsFolder.getAbsolutePath());

        int throttleInKbPerSec = _nodeConf.getShardDeployThrottle();

        //初始化ShardManager，ShardManager是复制文件的管理
        final ShardManager shardManager;
        if (throttleInKbPerSec > 0) {
            LOG.info("throtteling of shard deployment to " + throttleInKbPerSec + " kilo-bytes per second");
            shardManager = new ShardManager(
                    _nodeConf,
                    shardsFolder,
                    (IndexUpdateListener)_contentServer,
                    new ThrottledInputStream.ThrottleSemaphore(throttleInKbPerSec * 1024));
        } else {
            shardManager = new ShardManager(_nodeConf, shardsFolder, (IndexUpdateListener)_contentServer);
        }

        //Node的上下文对象。保存着Node有用的实例
        _context = new NodeContext(_protocol, this, shardManager, _contentServer);


        //在ZooKeeper中注册当前节点的信息
        _protocol.registerComponent(this);

        //开始监控， 也意味着该节点在ZooKeeper中生效, 把当前节点信息发送到监控中心。
        startMonitor(_nodeName, _nodeConf);

        //其它初始化
        init();
        LOG.info("started node '" + _nodeName + "'");
    }


    /**
     * 其他功能的初始化
     */
    private synchronized void init() {
        //重新把以前注册的索引地址重新部署
        redeployInstalledShards();


        //启动元信息
        NodeMetaData nodeMetaData = new NodeMetaData(_nodeName);


        //在ZooKeeper中写入当前节点的启动信息
        NodeQueue nodeOperationQueue = _protocol.publishNode(this, nodeMetaData);


        //启动节点其他操作
        startOperatorThread(nodeOperationQueue);
    }


    /**
     * 启动节点其他操作
     * @param nodeOperationQueue
     */
    private void startOperatorThread(NodeQueue nodeOperationQueue) {
        _nodeOperatorThread = new Thread(new NodeOperationProcessor(nodeOperationQueue, _context));
        _nodeOperatorThread.setName(NodeOperationProcessor.class.getSimpleName() + ": " + getName());
        _nodeOperatorThread.setDaemon(true);
        _nodeOperatorThread.start();
    }

    @Override
    public synchronized void reconnect() {
        LOG.info(_nodeName + " reconnected");
        init();
    }

    @Override
    public synchronized void disconnect() {
        if (_nodeOperatorThread == null) {
            LOG.warn(_nodeName + " disconnected before initialization complete");
            return;
        }
        LOG.info(_nodeName + " disconnected");
        try {
            do {
                LOG.info("trying to stop node-processor...");
                _nodeOperatorThread.interrupt();
                _nodeOperatorThread.join(2500);
            } while (_nodeOperatorThread.isAlive());
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
        // we keep serving the shards
    }


    /**
     * 重新部署加载上一次的shards
     */
    private void redeployInstalledShards() {
        Collection<String> installedShards = _context.getShardManager().getInstalledShards();
        LOG.info("redeploy installed shards: " + installedShards);
        ShardRedeployOperation redeployOperation = new ShardRedeployOperation(installedShards);
        try {
            //重新发布所有部署的 shard
            redeployOperation.execute(_context);
        } catch (InterruptedException e) {
            ExceptionUtil.convertToRuntimeException(e);
        }
    }


    /**
     * 开始监控。 把监控信息写入ZooKeeper
     * @param nodeName
     * @param conf
     */
    private void startMonitor(String nodeName, NodeConfiguration conf) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("starting node monitor");
        }
        String monitorClass = conf.getMonitorClass();
        try {
            Class<?> c = Class.forName(monitorClass);
            _monitor = (IMonitor) c.newInstance();
            _monitor.startMonitoring(nodeName, _protocol);
        } catch (Exception e) {
            LOG.error("Unable to start node monitor:", e);
        }
    }


    /**
     * 关闭节点Node。 停止各种服务
     */
    public void shutdown() {
        if (_stopped) {
            throw new IllegalStateException("already stopped");
        }
        LOG.info("shutdown " + _nodeName + " ...");
        _stopped = true;

        if (_monitor != null) {
            _monitor.stopMonitoring();
        }
        LOG.info("stoped monitor...");

        try {
            _nodeOperatorThread.interrupt();
            LOG.info("stoped node operator thread...");
        } catch (Exception e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        }

        ExecutorService executor = Executors.newCachedThreadPool();
        Future future = executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    _protocol.unregisterComponent(Node.this);
                    LOG.info("unregistered " + _nodeName + " node from zookeeper...");
                } catch (Exception e) {
                    LOG.debug(ExceptionUtils.getMessage(e));
                }
            }
        });

        try {
            future.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        }

        executor.shutdownNow();
        try {
            _rpcServer.stop();
            LOG.info("stoped rpc service...");
        } catch (Exception e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        }

        try {
            _context.getContentServer().shutdown();
        } catch (Throwable t) {
            LOG.error("Error shutting down server", t);
        }

        LOG.info("shutdown " + _nodeName + " finished");
    }

    public String getName() {
        return _nodeName;
    }

    public NodeContext getContext() {
        return _context;
    }

    public int getRPCServerPort() {
        return _rpcServer.getListenerAddress().getPort();
    }

    public boolean isRunning() {
        return _context != null && !_stopped;
    }

    public void join() throws InterruptedException {
        _rpcServer.join();
    }

    public Server getRpcServer() {
        return _rpcServer;
    }

    /*
     * Starting the hadoop RPC server that response to query requests. We iterate
     * over a port range of node.server.port.start + 10000
     */
    private static Server startRPCServer(String hostName,
                                         int startPort,
                                         IContentServer nodeManaged,
                                         int handlerCount) {
        int serverPort = startPort;
        Server rpcServer = null;
        while (rpcServer == null) {
            try {
                rpcServer = new RPC.Builder(HadoopUtil.getHadoopConf())
                        .setProtocol(ILuceneServer.class)
                        .setInstance(nodeManaged)
                        .setBindAddress(hostName)
                        .setPort(serverPort)
                        .setNumHandlers(handlerCount)
                        .build();

                LOG.info(nodeManaged.getClass().getSimpleName() +
                        " server started on : " +
                        hostName + ":" + serverPort);
            } catch (final BindException e) {
                throw new IllegalArgumentException(e);
            } catch (final IOException e) {
                throw new IllegalStateException("unable to create rpc server", e);
            }
        }
        try {
            rpcServer.start();
        } catch (final Exception e) {
            throw new RuntimeException("failed to start rpc server", e);
        }
        return rpcServer;
    }


    @Override
    public String toString() {
        return _nodeName;
    }

    public InteractionProtocol getProtocol() {
        return _protocol;
    }

}
