package com.ivyft.katta.server;

import com.ivyft.katta.lib.lucene.FreeSocketPortFactory;
import com.ivyft.katta.lib.lucene.SocketPortFactory;
import com.ivyft.katta.lib.lucene.SolrHandler;
import com.ivyft.katta.node.IContentServer;
import com.ivyft.katta.node.ShardManager;
import com.ivyft.katta.server.lucene.KattaLuceneServer;
import com.ivyft.katta.server.protocol.KattaServerProtocol;
import com.ivyft.katta.util.HadoopUtil;
import com.ivyft.katta.util.NetworkUtils;
import com.ivyft.katta.util.NodeConfiguration;
import com.ivyft.katta.util.ThrottledInputStream;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.BindException;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/2/26
 * Time: 13:38
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaBooter {


    public final static String KATTA_SERVER_PORT = "katta.single.server.port";

    /**
     * conf
     *
     */
    private final NodeConfiguration _nodeConf;


    /**
     * lucene server
     */
    private final KattaLuceneServer luceneServer;


    /**
     * 是一个机器名+RPCPort的字符串。标识当前的机器
     */
    protected String _nodeName;



    /**
     * 当前的节点停止OR正在运行
     */
    private boolean _stopped;


    protected Server rpcServer;


    private static Logger LOG = LoggerFactory.getLogger(KattaBooter.class);


    public KattaBooter(NodeConfiguration nodeConfiguration, KattaLuceneServer server) {
        this._nodeConf = nodeConfiguration;
        this.luceneServer = server;
    }





    /**
     * 启动Node
     */
    public void start() {
        if (_stopped) {
            throw new IllegalStateException("Node cannot be started again after it was shutdown.");
        }
        LOG.info("starting rpc server with  server class = " + luceneServer.getClass().getCanonicalName());


        String hostName = NetworkUtils.getLocalhostName();

        int port = _nodeConf.getInt(KATTA_SERVER_PORT, _nodeConf.getStartPort());
        SocketPortFactory factory = new FreeSocketPortFactory();
        int step = _nodeConf.getInt(KATTA_SERVER_PORT + ".step", 1);
        port = factory.getSocketPort(port, step);

        //启动Hadoop 的RPC
        rpcServer = startRPCServer(
                hostName,
                port,
                luceneServer,
                _nodeConf.getRpcHandlerCount());

        //把新端口号写到 NODE
        _nodeConf.setProperty(KATTA_SERVER_PORT, port);

        _nodeName = hostName + ":" + port;

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
                    luceneServer,
                    new ThrottledInputStream.ThrottleSemaphore(throttleInKbPerSec * 1024));
        } else {
            shardManager = new ShardManager(_nodeConf, shardsFolder, luceneServer);
        }

        luceneServer.setShardManager(shardManager);

        luceneServer.init(_nodeName, _nodeConf);


        LOG.info("started node '" + _nodeName + "'");
    }


    /*
     * Starting the hadoop RPC server that response to query requests. We iterate
     * over a port range of node.server.port.start + 10000
     */
    private static RPC.Server startRPCServer(String hostName,
                                             int startPort,
                                             IContentServer nodeManaged,
                                             int handlerCount) {
        int serverPort = startPort;
        RPC.Server rpcServer = null;
        while (rpcServer == null) {
            try {
                rpcServer = new RPC.Builder(HadoopUtil.getHadoopConf())
                        .setProtocol(KattaServerProtocol.class)
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



    public void serve() throws InterruptedException {
        rpcServer.join();
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

        LOG.info("stoped monitor...");

        try {
            rpcServer.stop();
            LOG.info("stoped rpc service...");
        } catch (Exception e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        }

        try {
            luceneServer.shutdown();
        } catch (Throwable t) {
            LOG.error("Error shutting down server", t);
        }

        LOG.info("shutdown " + _nodeName + " finished");
    }



    public static void main(String[] args) throws Exception {
        NodeConfiguration nodeConfiguration = new NodeConfiguration();;
        SolrHandler.init(nodeConfiguration.getFile("node.solrhome.folder"));

        KattaLuceneServer server = new KattaLuceneServer();
        final KattaBooter node = new KattaBooter(nodeConfiguration, server);
        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                node.shutdown();
            }
        });
        node.serve();
    }
}
