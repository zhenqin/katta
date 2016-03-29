package com.ivyft.katta.node;

import com.ivyft.katta.lib.writer.ShardRange;
import com.ivyft.katta.operation.master.IndexMergeOperation;
import com.ivyft.katta.protocol.CommitShards;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.MasterConfiguration;
import com.ivyft.katta.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;

import java.util.HashSet;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/3/25
 * Time: 12:55
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class OperationMaster {


    public static void main(String[] args) {
        ZkConfiguration zkConf = new ZkConfiguration();

        ZkClient zkClient = new ZkClient(zkConf.getZKServers());


        InteractionProtocol protocol = new InteractionProtocol(zkClient, zkConf);
        MasterConfiguration conf = new MasterConfiguration();;

        CommitShards commitShards = new CommitShards("userindex", "hello", new HashSet<ShardRange>());
        protocol.addMasterOperation(new IndexMergeOperation(commitShards));
    }
}
