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

import com.ivyft.katta.protocol.ConnectedComponent;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.util.ZkConfiguration.PathDef;
import org.I0Itec.zkclient.IZkDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
public class IndexDeployFuture implements IIndexDeployFuture, IZkDataListener, ConnectedComponent {

    private static Logger log = LoggerFactory.getLogger(IndexDeployFuture.class);

    private final InteractionProtocol _protocol;

    /**
     *
     */
    private final String _indexName;


    /**
     *
     *
     *
     */
    private volatile boolean _registered;


    /**
     *
     * @param protocol 协议
     * @param indexName index name
     */
    public IndexDeployFuture(InteractionProtocol protocol, String indexName) {
        _protocol = protocol;
        _indexName = indexName;
        _protocol.registerComponent(this);
        _protocol.registerDataListener(this, PathDef.INDICES_METADATA, indexName, this);
        _registered = true;
    }


    /**
     *
     * @return
     */
    public synchronized IndexState getState() {
        if (!_registered) {
            return IndexState.DEPLOYED;
        }

        IndexMetaData indexMD = _protocol.getIndexMD(_indexName);
        if (indexMD == null) {
            return IndexState.DEPLOYING;
        } else if (indexMD.hasDeployError()) {
            return IndexState.ERROR;
        }
        if (_registered) {
            _registered = false;
            _protocol.unregisterComponent(this);
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
        while (isDeploymentRunning()) {
            wait(maxTime);
            maxTime = maxTime - (System.currentTimeMillis() - startJoin);
            if (maxTime <= 0) {
                break;
            }
        }
        return getState();
    }

    @Override
    public synchronized void handleDataChange(String dataPath, Object data) {
        wakeSleeper();
    }

    public synchronized void handleDataDeleted(String dataPath) {
        _registered = false;
        _protocol.unregisterComponent(this);
        wakeSleeper();
    }

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
