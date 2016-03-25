package com.ivyft.katta.node;

import com.ivyft.katta.operation.node.NodeOperationProcessor;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.NodeQueue;
import com.ivyft.katta.protocol.metadata.NodeMetaData;
import com.ivyft.katta.util.NetworkUtils;
import com.ivyft.katta.util.NodeConfiguration;
import com.ivyft.katta.util.ThrottledInputStream;
import com.ivyft.katta.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/3/25
 * Time: 11:55
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class OperationNode {

    static Logger LOG = LoggerFactory.getLogger(OperationNode.class);

    public static void main(String[] args) throws InterruptedException {
        ZkConfiguration zkConf = new ZkConfiguration();

        ZkClient zkClient = new ZkClient(zkConf.getZKServers());


        InteractionProtocol protocol = new InteractionProtocol(zkClient, zkConf);
        NodeConfiguration nodeConfiguration = new NodeConfiguration();

        String hostName = NetworkUtils.getLocalhostName();
        String nodeName = hostName + ":20000";

        // we add hostName and port to the shardFolder to allow multiple nodes per
        // server with the same configuration
        File shardsFolder = new File(nodeConfiguration.getShardFolder(), nodeName.replaceAll(":", "_"));
        LOG.info("local shard folder: " + shardsFolder.getAbsolutePath());

        int throttleInKbPerSec = nodeConfiguration.getShardDeployThrottle();

        //初始化ShardManager，ShardManager是复制文件的管理
        final ShardManager shardManager;
        if (throttleInKbPerSec > 0) {
            LOG.info("throtteling of shard deployment to " + throttleInKbPerSec + " kilo-bytes per second");
            shardManager = new ShardManager(shardsFolder,
                    new ThrottledInputStream.ThrottleSemaphore(throttleInKbPerSec * 1024));
        } else {
            shardManager = new ShardManager(shardsFolder);
        }


        //启动元信息
        NodeMetaData nodeMetaData = new NodeMetaData(nodeName);
        //在ZooKeeper中写入当前节点的启动信息


        //在Livenode下写入临时的节点信息
        String nodePath = zkConf.getZkPath(ZkConfiguration.PathDef.NODES_LIVE, nodeName);

        // create queue for incoming node operations
        String queuePath = zkConf.getZkPath(ZkConfiguration.PathDef.NODE_QUEUE, nodeName);
        if (zkClient.exists(queuePath)) {
            zkClient.deleteRecursive(queuePath);
        }

        NodeQueue nodeQueue = new NodeQueue(zkClient, queuePath);

        // mark the node as connected
        if (zkClient.exists(nodePath)) {
            LOG.warn("Old node ephemeral '" + nodePath + "' detected, deleting it...");
            zkClient.delete(nodePath);
        }

        //创建临时节点。并把节点注册信息保存到_zkEphemeralPublishesByComponent
        zkClient.createEphemeral(nodePath, null);

        //NodeQueue nodeOperationQueue = protocol.publishNode(this, nodeMetaData);

        Thread _nodeOperatorThread = new Thread(new NodeOperationProcessor(nodeQueue, null));
        _nodeOperatorThread.setName(NodeOperationProcessor.class.getSimpleName());
        _nodeOperatorThread.setDaemon(true);
        _nodeOperatorThread.start();

        _nodeOperatorThread.join();
    }
}
