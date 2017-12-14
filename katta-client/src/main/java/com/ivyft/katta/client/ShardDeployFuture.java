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

import com.ivyft.katta.operation.master.AbstractIndexOperation;
import com.ivyft.katta.protocol.ConnectedComponent;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.protocol.metadata.Shard;
import com.ivyft.katta.util.ZkConfiguration;
import com.ivyft.katta.util.ZkConfiguration.PathDef;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 *
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
public class ShardDeployFuture implements IIndexDeployFuture, IZkDataListener, ConnectedComponent {

    private static Logger log = LoggerFactory.getLogger(ShardDeployFuture.class);

    private final InteractionProtocol _protocol;

    /**
     * Add Shard 的 Index
     */
    private final String _indexName;


    /**
     * Add Shard Name
     */
    private final String shardName;



    /**
     * 初始化的值
     */
    private AtomicBoolean change = new AtomicBoolean(false);


    /**
     *
     * @param protocol 协议
     * @param indexName index name
     */
    public ShardDeployFuture(InteractionProtocol protocol, String indexName,
                             IndexMetaData metaData, String shardPath) {
        _protocol = protocol;
        _indexName = indexName;

        String trim = StringUtils.trim(shardPath);

        boolean b = trim.endsWith("/");
        while (b) {
            // 循环去掉最后的 /，防止有人添加 shard 填写 path 时带有最后的 /.
            // 可能的如： /path/dir/for/， /path/dir/bar//
            trim = trim.substring(0, trim.length() - 1);
            b = trim.endsWith("/");
        }

        //如 /path/dir/for 取得最后的 for
        String name = trim.substring(trim.lastIndexOf("/") + 1, trim.length());
        this.shardName = this._indexName + AbstractIndexOperation.INDEX_SHARD_NAME_SEPARATOR + name;

        Shard shard = metaData.getShard(shardName);
        if(shard != null) {
            throw new IllegalArgumentException("Index with name " + name + "'s shard " +
                    shardName +" was exists.");
        }

        //监控 Index 的数据变化
        _protocol.registerDataListener(this,
                ZkConfiguration.PathDef.INDICES_METADATA, this._indexName,
                this);

        //旧程序，有 Bug，在程序当前很可能还没有该路径
        //_protocol.registerChildListener(this, PathDef.SHARD_TO_NODES, this._indexName +
        //        AbstractIndexOperation.INDEX_SHARD_NAME_SEPARATOR + this.shardName, this);
    }


    /**
     *
     * @return
     */
    public synchronized IndexState getState() {
        if (!change.get()) {
            return IndexState.DEPLOYING;
        }

        _protocol.unregisterComponent(this);

        List<String> shardNodes = _protocol.getShardNodes(shardName);
        if (shardNodes == null || shardNodes.isEmpty()) {
            return IndexState.ERROR;
        }
        return IndexState.DEPLOYED;
    }

    private boolean isDeploymentRunning() {
        return getState() == IndexState.DEPLOYING;
    }

    public synchronized IndexState joinDeployment() throws InterruptedException {
        while (isDeploymentRunning()) {
            wait(3000);
        }
        return getState();
    }

    public synchronized IndexState joinDeployment(long maxTime) throws InterruptedException {
        long startJoin = System.currentTimeMillis();

        Exception exception;
        try {
            while (isDeploymentRunning()) {
                wait(maxTime);
                maxTime = maxTime - (System.currentTimeMillis() - startJoin);
                if (maxTime <= 0) {
                    break;
                }
            }
        } catch (Exception e) {
            exception = e;
        }
        return getState();
    }


    @Override
    public void handleDataChange(String s, Object o) throws Exception {
        if(o instanceof IndexMetaData) {
            IndexMetaData indexMetaData = (IndexMetaData)o;
            Shard shard = indexMetaData.getShard(shardName);

            change.set(shard != null);
        }

        wakeSleeper();
    }

    @Override
    public void handleDataDeleted(String s) throws Exception {
        wakeSleeper();
    }


    /*
    //旧程序是监控 path: /katta/shard-to-nodes/test#index1 的变化
    public void added(String name) {
        change.set(true);
        wakeSleeper();
    }

    public void removed(String name) {
        wakeSleeper();
    }
    */

    private synchronized void wakeSleeper() {
        notifyAll();
    }

    @Override
    public void reconnect() {
        // sg: we just want to make sure we get the very latest state of the
        // index, since we might missed a event. With zookeeper 3.x we should still
        // have subscribed notifications and don't need to re-subscribe
        log.warn("Reconnecting IndexDeployFuture");
        wakeSleeper();
    }

    @Override
    public void disconnect() {
        // nothing
    }

}
