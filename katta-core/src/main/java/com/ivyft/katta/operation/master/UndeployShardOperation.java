package com.ivyft.katta.operation.master;

import com.ivyft.katta.master.MasterContext;
import com.ivyft.katta.operation.OperationId;
import com.ivyft.katta.operation.node.OperationResult;
import com.ivyft.katta.operation.node.ShardUndeployOperation;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.protocol.metadata.Shard;
import com.ivyft.katta.util.CollectionUtil;
import com.ivyft.katta.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 *
 * 删除一个索引库Index 的分片 Shard
 *
 *
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 17/12/14
 * Time: 12:17
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class UndeployShardOperation extends AbstractIndexOperation {

    private static final long serialVersionUID = 1L;



    protected static Logger LOG = LoggerFactory.getLogger(UndeployShardOperation.class);

    /**
     * 要卸载、删除的Index Name
     */
    private String indexName;


    /**
     * 待移除的 ShardName
     */
    private String shardName;


    public UndeployShardOperation(String indexName, String shardName) {
        this.indexName = indexName;
        this.shardName = shardName;
    }


    /**
     * 如果队列列表中的待执行操作和当前操作相同，则取消
     * @param runningOperations 队列中需要的操作
     * @return 返回是否执行
     * @throws Exception
     */
    @Override
    public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
        for (MasterOperation operation : runningOperations) {
            if (operation instanceof UndeployShardOperation) {
                UndeployShardOperation undeployShardOperation = (UndeployShardOperation) operation;
                if(undeployShardOperation.indexName.equals(this.indexName) &&
                        undeployShardOperation.shardName.equals(this.shardName))
                return ExecutionInstruction.CANCEL;
            }
        }
        return ExecutionInstruction.EXECUTE;
    }

    @Override
    public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
        InteractionProtocol protocol = context.getProtocol();
        IndexMetaData indexMD = protocol.getIndexMD(this.indexName);

        Shard shard = indexMD.getShard(shardName);

        //该 shard 部署在的 Node，找出来
        Map<String, List<String>> shard2NodesMap = protocol.getShard2NodesMap(Arrays.asList(shardName));

        // 转换为 Node，上对应的 shard， list 值只有一个
        Map<String, List<String>> node2ShardsMap = CollectionUtil.invertListMap(shard2NodesMap);

        // 所有部署 shardName 的 Node
        Set<String> nodes = node2ShardsMap.keySet();
        List<OperationId> nodeOperationIds = new ArrayList<OperationId>(nodes.size());
        for (String node : nodes) {
            List<String> nodeShards = node2ShardsMap.get(node);
            //给负载Index Shard的每个节点发送一个卸载的消息
            OperationId operationId = protocol.addNodeOperation(node, new ShardUndeployOperation(nodeShards));
            nodeOperationIds.add(operationId);
        }
        return nodeOperationIds;
    }


    /**
     * 执行结束后如何操作
     * @param context
     * @param results
     * @throws Exception
     */
    @Override
    public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
        LOG.info("remove shard " + indexName + "/" + shardName + " to complete");
        IndexMetaData data = context.getProtocol().getIndexMD(indexName);
        if(data != null) {
            Shard shard = data.getShard(shardName);
            data.removeShard(shard);

            InteractionProtocol protocol = context.getProtocol();
            ZkClient zkClient = protocol.getZkClient();
            ZkConfiguration zkConf = protocol.getZkConfiguration();
            zkClient.deleteRecursive(zkConf.getZkPath(ZkConfiguration.PathDef.SHARD_TO_NODES, shard.getName()));

            protocol.updateIndexMD(data);
        }
    }
}
