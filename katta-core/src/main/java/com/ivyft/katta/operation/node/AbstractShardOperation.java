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
import org.I0Itec.zkclient.ExceptionUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 *
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
public abstract class AbstractShardOperation implements NodeOperation {

    /**
     * 序列化
     */
    private static final long serialVersionUID = 1L;



    public final static String SHARD_PATH = "shard-path";




    public final static String COLLECTION_NAME = "collection-name";


    /**
     * shard Name ---> shard Path
     */
    protected Map<String, Map<String, Object>> shardPathesByShardNames = new HashMap<String, Map<String, Object>>(3);


    /**
     * Log
     */
    private final static Logger LOG = LoggerFactory.getLogger(AbstractShardOperation.class);


    /**
     * 默认的构造方法
     */
    protected AbstractShardOperation() {
    }


    @Override
    public final DeployResult execute(final NodeContext context) throws InterruptedException {
        final DeployResult result = new DeployResult(context.getNode().getName());
        LOG.info("operating all shards:" + getShardNames());
        ExecutorService threadPool = Executors.newCachedThreadPool();
        try {
            for (final String shardName : getShardNames()) {
                try {
                    LOG.info(getOperationName() + " shard '" + shardName + "'");
                    Future<?> submit = threadPool.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                execute(context, shardName, result);
                            } catch (Exception e) {
                                LOG.error(ExceptionUtils.getFullStackTrace(e));
                                throw new IllegalStateException(e);
                            }
                        }
                    });

                    submit.get(context.getInt("katta.opt.single.shard.timeout.sec", 3 * 60), TimeUnit.SECONDS);
                    LOG.info(getOperationName() + " shard '" + shardName + "'" + " success.");
                } catch (Exception e) {
                    LOG.error("failed to " + getOperationName() + " shard '" + shardName + "' on node '"
                            + context.getNode().getName() + "'", e);
                    ExceptionUtil.rethrowInterruptedException(e);
                    result.addShardException(shardName, e);
                    onException(context, shardName, e);
                }
            }
        } finally {
            try {
                threadPool.shutdown();
            } catch (Exception e) {

            }
        }

        return result;
    }

    protected abstract String getOperationName();

    protected abstract void execute(NodeContext context, String shardName, DeployResult result) throws Exception;

    protected abstract void onException(NodeContext context, String shardName, Exception e);


    /**
     * 给Shard写入Node持有的信息
     * @param shardName shard名称
     * @param context Context
     */
    protected void publishShard(String shardName, NodeContext context) {
        LOG.info("publish shard '" + shardName + "'");
        context.getProtocol().publishShard(context.getNode(), shardName);
    }



    public Set<String> getShardNames() {
        return shardPathesByShardNames.keySet();
    }

    public String getShardPath(String shardName) {
        Map<String, Object> map = shardPathesByShardNames.get(shardName);
        if(map != null) {
            return (String)map.get(SHARD_PATH);
        }
        return null;
    }



    public String getCollectionName(String shardName) {
        Map<String, Object> map = shardPathesByShardNames.get(shardName);
        if(map != null) {
            return (String)map.get(COLLECTION_NAME);
        }
        return null;
    }


    public void addShard(String shardName, String shardPath, String collectionName) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(SHARD_PATH, shardPath);
        map.put(COLLECTION_NAME, collectionName);

        shardPathesByShardNames.put(shardName, map);
    }


    public void addShard(String shardName, String collectionName) {
        Map<String, Object> map = shardPathesByShardNames.get(shardName);
        if(map != null) {
            map.put(COLLECTION_NAME, collectionName);
            shardPathesByShardNames.put(shardName, map);
        } else {
            map = new HashMap<String, Object>();
            map.put(COLLECTION_NAME, collectionName);
            shardPathesByShardNames.put(shardName, map);
        }
    }


    public void addShard(String shardName) {
        Map<String, Object> map = shardPathesByShardNames.get(shardName);
        shardPathesByShardNames.put(shardName, map);
    }


    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + Integer.toHexString(hashCode()) + ":" + getShardNames();
    }

}
