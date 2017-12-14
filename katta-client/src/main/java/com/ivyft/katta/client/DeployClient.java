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

import com.ivyft.katta.operation.master.*;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.protocol.metadata.NewIndexMetaData;
import com.ivyft.katta.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;

import java.util.List;


/**
 *
 *
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
public class DeployClient implements IDeployClient {


    /**
     * 操作的ZK协议
     */
    private final InteractionProtocol protocol;

    /**
     * @deprecated use {@link #DeployClient(InteractionProtocol)} instead
     */
    public DeployClient(ZkClient zkClient, ZkConfiguration configuration) {
        this(new InteractionProtocol(zkClient, configuration));
    }



    public DeployClient(InteractionProtocol interactionProtocol) {
        this.protocol = interactionProtocol;
    }




    /**
     * 创建一个索引集
     * @param indexName 索引集名称
     * @param indexPath 索引集路径
     * @return 返回创建索引句柄
     *
     */
    @Override
    public IIndexDeployFuture createIndex(String indexName,
                                          String indexPath,
                                          int shardNum,
                                          int shardStep) {
        //验证索引, 及名称String indexName, String path
        //validateIndexData(indexName, collectionName, replicationLevel);

        NewIndexMetaData newIndexMetaData = new NewIndexMetaData(indexName, indexPath, shardNum, shardStep);

        //部署结果如何?
        CreatedIndexDeployFuture deployFuture = new CreatedIndexDeployFuture(newIndexMetaData);

        //尝试创建索引
        protocol.createIndex(newIndexMetaData);

        return deployFuture;
    }


    /**
     * 部署一份索引.
     * @param indexName
     * @param indexPath
     * @param replicationLevel 复制级别
     * @return
     */
    @Override
    public IIndexDeployFuture addIndex(String indexName,
                                       String indexPath,
                                       String collectionName,
                                       int replicationLevel) {
        //验证索引, 及名称
        validateIndexData(indexName, collectionName, replicationLevel);

        //尝试部署索引
        protocol.addMasterOperation(
                new IndexDeployOperation(indexName, indexPath,
                        collectionName, replicationLevel));

        //部署结果如何?
        return new IndexDeployFuture(protocol, indexName);
    }



    @Override
    public IIndexDeployFuture addShard(String indexName,
                                       String shardPath) {
        //验证索引, 及名称
        validateIndexData(indexName, shardPath);


        IndexMetaData indexMetaData = protocol.getIndexMD(indexName);
        if(indexMetaData == null) {
            throw new IllegalArgumentException("it has not index name: " + indexName);
        }

        // 选绑定，监控 ZooKeeper 节点数据变化
        ShardDeployFuture shardDeployFuture = new ShardDeployFuture(protocol, indexName, indexMetaData, shardPath);

        //尝试添加一个Shard
        protocol.addMasterOperation(
                new ShardDeployOperation(indexName, shardPath,
                indexMetaData.getCollectionName(), indexMetaData.getReplicationLevel()));

        return shardDeployFuture;
    }




    private void validateIndexData(String name, String shardPath) {
        if (name.trim().equals("*")) {
            throw new IllegalArgumentException("invalid index name: " + name);
        }
    }


    /**
     * 验证索引名称合格否? 不能包含*, #等字符
     * @param name 索引名称
     * @param collectionName
     * @param replicationLevel 复制级别
     */
    private void validateIndexData(String name, String collectionName, int replicationLevel) {
        if (replicationLevel <= 0) {
            throw new IllegalArgumentException("replication level must be 1 or greater");
        }

        //名字不能是*
        if (name.trim().equals("*")) {
            throw new IllegalArgumentException("invalid index name: " + name);
        }

        //不能包含#
        if (name.contains(AbstractIndexOperation.INDEX_SHARD_NAME_SEPARATOR + "")) {
            throw new IllegalArgumentException("invalid index name : " + name + " - must not contain "
                    + AbstractIndexOperation.INDEX_SHARD_NAME_SEPARATOR);
        }
    }

    @Override
    public void removeIndex(String indexName) {
        if (!existsIndex(indexName)) {
            throw new IllegalArgumentException("index with name '" + indexName + "' does not exists");
        }
        protocol.addMasterOperation(new IndexUndeployOperation(indexName));
    }


    public void removeShard(String indexName, String shardName) {
        
    }

    @Override
    public boolean existsIndex(String indexName) {
        return protocol.indexExists(indexName);
    }


    @Override
    public boolean existsNewIndex(String name) {
        return protocol.getNewIndexs().contains(name);
    }

    @Override
    public IndexMetaData getIndexMetaData(String indexName) {
        return protocol.getIndexMD(indexName);
    }

    @Override
    public List<String> getIndices() {
        return protocol.getIndices();
    }
}
