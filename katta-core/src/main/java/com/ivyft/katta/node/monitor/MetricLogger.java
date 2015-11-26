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
package com.ivyft.katta.node.monitor;

import com.ivyft.katta.protocol.ConnectedComponent;
import com.ivyft.katta.protocol.IAddRemoveListener;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.ZkConfiguration;
import org.I0Itec.zkclient.IZkDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;


/**
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
public class MetricLogger implements IZkDataListener, ConnectedComponent {

    public enum OutputType {
        Log4J,

        SysOut;
    }


    /**
     * Log
     */
    private final static Logger LOG = LoggerFactory.getLogger(MetricLogger.class);


    /**
     * 输出类ixng
     */
    private OutputType outputType;


    /**
     * 同步锁
     */
    private ReentrantLock lock;

    /**
     * ZK 操作
     */
    protected final InteractionProtocol protocol;


    /**
     * count
     */
    private long loggedRecords = 0;


    /**
     *
     * @param outputType
     * @param protocol
     */
    public MetricLogger(OutputType outputType, InteractionProtocol protocol) {
        this.protocol = protocol;
        this.outputType = outputType;
        this.protocol.registerComponent(this);
        List<String> children = this.protocol.registerChildListener(this,
                ZkConfiguration.PathDef.NODE_METRICS,
                new IAddRemoveListener() {
                    @Override
                    public void removed(String name) {
                        unsubscribeDataUpdates(name);
                    }

                    @Override
                    public void added(String name) {
                        MetricsRecord metric = MetricLogger.this.protocol.getMetric(name);
                        logMetric(metric);
                        subscribeDataUpdates(name);
                    }
                });
        for (String node : children) {
            subscribeDataUpdates(node);
        }
        this.lock = new ReentrantLock();
        this.lock.lock();
    }

    protected void subscribeDataUpdates(String nodeName) {
        this.protocol.registerDataListener(this, ZkConfiguration.PathDef.NODE_METRICS, nodeName, this);
    }

    protected void unsubscribeDataUpdates(String nodeName) {
        this.protocol.unregisterDataChanges(this, ZkConfiguration.PathDef.NODE_METRICS, nodeName);
    }

    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
        MetricsRecord metrics = (MetricsRecord) data;
        logMetric(metrics);
    }

    protected void logMetric(MetricsRecord metrics) {
        switch (this.outputType) {
            case Log4J:
                LOG.info(metrics.toString());
                break;
            case SysOut:
                System.out.println(metrics.toString());
                break;
            default:
                throw new IllegalStateException("output type " + this.outputType + " not supported");
        }
        loggedRecords++;
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
        // nothing to do
    }

    public void join() throws InterruptedException {
        synchronized (this.lock) {
            this.lock.wait();
        }
    }

    public long getLoggedRecords() {
        return this.loggedRecords;
    }

    @Override
    public void disconnect() {
        this.lock.unlock();
    }

    @Override
    public void reconnect() {
        // nothing to do
    }

}
