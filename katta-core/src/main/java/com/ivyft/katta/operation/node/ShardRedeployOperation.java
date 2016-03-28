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
package com.ivyft.katta.operation.node;

import com.ivyft.katta.node.IContentServer;
import com.ivyft.katta.node.NodeContext;
import com.ivyft.katta.node.ShardManager;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.Collection;
import java.util.Map;

/**
 * <p>
 *
 * Redploys shards which are already installed in {@link com.ivyft.katta.node.ShardManager}.
 * 重新部署/加载一个shard索引
 *
 * </p>
 *
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-19
 * Time: 上午8:59
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class ShardRedeployOperation extends AbstractShardOperation {


    /**
     * 序列化
     */
    private static final long serialVersionUID = 1L;


    /**
     * Log
     */
    private final static Logger log = LoggerFactory.getLogger(ShardRedeployOperation.class);


    /**
     * 构造方法
     * @param installedShards 所有需要重新加载的Shard集合
     */
    public ShardRedeployOperation(Collection<String> installedShards) {
        for (String shardName : installedShards) {
            addShard(shardName);
        }
    }


    /**
     * 重新加载的命令
     * @return
     */
    @Override
    protected String getOperationName() {
        return "redeploy";
    }

    @Override
    protected void execute(NodeContext context, String shardName, DeployResult deployResult) throws Exception {
        //不需要重新安裝，取得shard的本地目录
        File localShardFolder = context.getShardManager().getShardFolder(shardName);

        String index = shardName.substring(0, shardName.indexOf("#"));

        IndexMetaData metaData = context.getProtocol().getIndexMD(index);
        //TODO 换了 ZooKeeper, 但是没有删除 katta-shard dir, 会在这里报错
        if(metaData == null) {
            log.info(index + " no registry from zookeeper, deleting " + shardName + " shard.");
            context.getShardManager().uninstallShard(shardName);
            log.info("deleted shard " + shardName + " success.");
            return;
        }

        String shardPath = metaData.getShardPath(shardName);

        log.info(getOperationName() + " local index, path: " + localShardFolder);
        addShard(shardName, shardPath, metaData.getCollectionName());

        //shardPath = getShardPath(shardName);
        URI shardFolder = new URI(shardPath);
        String scheme = shardFolder.getScheme();
        if(StringUtils.equals(ShardManager.HDFS, scheme)) {
            //TODO HDFS, 不需要安装, 这里尝试安装, 创建本地目录
            File file = new File(localShardFolder, ".ns.info");
            //判断是否有这个文件,有这个文件则表示索引被更改过,不能重新安装
            if(file.exists()) {
                //索引已经改变过,也就是 Katta 写过数据
            } else {
                //没有写过
                shardFolder = context.getShardManager().installShard(shardName, shardPath);
            }

        } else {
            //如果是本地文件, 走本地文件系统
            //检验是否需要重新安装
            if(context.getShardManager().validChanges(shardName, shardPath, localShardFolder)){
                log.info(shardName + " 不是最新的，將會刪除本地索引並且重新安裝。");
                //首先删除Shard占用的文件
                context.getShardManager().uninstallShard(shardName);
                log.info(shardName + " 重新安裝.");
                //然后重新安装
                shardFolder = context.getShardManager().installShard(shardName, shardPath);
            } else {
                log.info(shardName + "  path: " + shardPath + " 索引是最新的。");
            }
        }

        IContentServer contentServer = context.getContentServer();
        if (!contentServer.getShards().contains(shardName)) {

            String collectionName = this.getCollectionName(shardName);
            if(StringUtils.isBlank(collectionName)) {
                collectionName = readCollectionName(index, context);
                addShard(shardName, collectionName);
            }

            log.info(getOperationName() + " " + shardName + " use by collectionName: " + collectionName);

            //加载和重新加载,Solr collectionName传入
            contentServer.addShard(shardName, shardFolder, collectionName);

            Map<String, String> shardMetaData = context.getContentServer().getShardMetaData(shardName);
            if (shardMetaData == null) {
                throw new IllegalStateException("node managed '" + context.getContentServer()
                        + "' does return NULL as shard metadata");
            }

            deployResult.addShardMetaDataMap(shardName, shardMetaData);
        }

        //部署完成, 写入信息到Zookeeper
        publishShard(shardName, context);
    }


    /**
     * 以Shard读取Meta信息中的CollectionName
     * @param indexName
     * @param nodeContext context
     * @return 返回Index 的Solr Core Name
     */
    protected String readCollectionName(String indexName, NodeContext nodeContext) {
        IndexMetaData metaData = nodeContext.getProtocol().getIndexMD(indexName);
        if(metaData != null) {
            return metaData.getCollectionName();
        }
        return null;
    }



    @Override
    protected void onException(NodeContext context, String shardName, Exception e) {
        context.getShardManager().uninstallShard(shardName);
    }

}
