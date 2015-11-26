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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;


/**
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
public class ShardDeployOperation extends AbstractShardOperation {


    /**
     * 序列化
     */
    private static final long serialVersionUID = 1L;


    private static Logger log = LoggerFactory.getLogger(ShardDeployOperation.class);

    /**
     * default constructor
     */
    public ShardDeployOperation() {

    }


    @Override
    protected String getOperationName() {
        return "deploy";
    }

    @Override
    protected void execute(NodeContext context,
                           String shardName,
                           DeployResult deployResult) throws Exception {

        String shardPath = getShardPath(shardName);
        File localShardFolder = context.getShardManager().installShard(shardName, shardPath);
        log.info("copy shard " + shardName + " success. local: file://" + localShardFolder.getAbsolutePath());

        IContentServer contentServer = context.getContentServer();

        if (!contentServer.getShards().contains(shardName)) {

            log.info("=============================================");
            log.info(this.getCollectionName(shardName));
            log.info("=============================================");

            //Solr collectionName传入
            contentServer.addShard(shardName, localShardFolder, this.getCollectionName(shardName));

            Map<String, String> shardMetaData = context.getContentServer().getShardMetaData(shardName);
            if (shardMetaData == null) {
                throw new IllegalStateException("node managed '" + context.getContentServer()
                        + "' does return NULL as shard metadata");
            }
            deployResult.addShardMetaDataMap(shardName, shardMetaData);
        }
        publishShard(shardName, context);
    }

    @Override
    protected void onException(NodeContext context, String shardName, Exception e) {
        context.getShardManager().uninstallShard(shardName);
    }

}
