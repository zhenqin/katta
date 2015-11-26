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

import com.ivyft.katta.node.NodeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * <p>
 *     Node卸载Shard的消息
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
public class ShardUndeployOperation extends AbstractShardOperation {

    /**
     * 序列化表格
     */
    private static final long serialVersionUID = 1L;



    /**
     * Log
     */
    private final static Logger log = LoggerFactory.getLogger(ShardUndeployOperation.class);



    /**
     * 构造方法
     * @param shards
     */
    public ShardUndeployOperation(List<String> shards) {
        for (String shard : shards) {
            addShard(shard);
        }
    }


    /**
     * 返回卸载的string
     * @return
     */
    @Override
    protected String getOperationName() {
        return "undeploy";
    }


    @Override
    protected void execute(NodeContext context, String shardName, DeployResult deployResult) throws Exception {
        log.info("will " + getOperationName() + " shard: " + shardName);
        context.getProtocol().unpublishShard(context.getNode(), shardName);
        context.getContentServer().removeShard(shardName);
        context.getShardManager().uninstallShard(shardName);
        log.info(getOperationName() + " shard: " + shardName + " success.");
    }



    @Override
    protected void onException(NodeContext context, String shardName, Exception e) {
        context.getShardManager().uninstallShard(shardName);
    }


    @Override
    public String toString() {
        return getOperationName() + " shards: " + shardPathesByShardNames.keySet();
    }
}
