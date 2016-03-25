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
package com.ivyft.katta.master;

import com.ivyft.katta.operation.OperationId;
import com.ivyft.katta.operation.master.MasterOperation;
import com.ivyft.katta.operation.node.OperationResult;
import com.ivyft.katta.protocol.ConnectedComponent;
import com.ivyft.katta.protocol.IAddRemoveListener;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.ZkConfiguration.PathDef;
import org.I0Itec.zkclient.IZkDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * When watchdog for a list of {@link com.ivyft.katta.operation.node.NodeOperation}s. The watchdog is finished
 * if all operations are done or the nodes of the incomplete nodes went down.
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
public class OperationWatchdog implements ConnectedComponent, Serializable {

    /**
     * 序列化
     */
    private static final long serialVersionUID = 1L;


    /**
     *
     */
    private final String _queueElementId;


    /**
     *
     */
    private final List<OperationId> _openOperationIds;


    /**
     *
     */
    private final List<OperationId> _operationIds;

    /**
     * Master执行上下文
     */
    private MasterContext _context;


    /**
     * Master的操作
     */
    private final MasterOperation _masterOperation;


    /**
     * Log
     */
    protected final static Logger LOG = LoggerFactory.getLogger(OperationWatchdog.class);

    /**
     * 构造方法
     * @param queueElementId
     * @param masterOperation
     * @param operationIds
     */
    public OperationWatchdog(String queueElementId,
                             MasterOperation masterOperation,
                             List<OperationId> operationIds) {
        _queueElementId = queueElementId;
        _operationIds = operationIds;
        _masterOperation = masterOperation;
        _openOperationIds = new ArrayList<OperationId>(operationIds);
    }


    /**
     * 开始执行一个看门狗事件
     * @param context Master环境上下文
     */
    public void start(MasterContext context) {
        _context = context;
        subscribeNotifications();
    }


    /**
     * 发布事件
     */
    private final synchronized void subscribeNotifications() {

        //检查上一次没有完成的, 重新生成
        checkDeploymentForCompletion();
        if (isDone()) {
            return;
        }

        InteractionProtocol protocol = _context.getProtocol();
        protocol.registerChildListener(this, PathDef.NODES_LIVE, new IAddRemoveListener() {
            @Override
            public void removed(String name) {
                //移除节点? 重新部署?
                checkDeploymentForCompletion();
            }

            @Override
            public void added(String name) {
                // nothing to do
            }
        });
        IZkDataListener dataListener = new IZkDataListener() {
            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                checkDeploymentForCompletion();
            }

            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                // nothing to do
            }
        };
        for (OperationId operationId : _openOperationIds) {
            protocol.registerNodeOperationListener(this, operationId, dataListener);
        }
        checkDeploymentForCompletion();
    }


    /**
     * 检验完成?
     */
    protected final synchronized void checkDeploymentForCompletion() {
        //如果完成?
        if (isDone()) {
            return;
        }

        //获得活着的Node
        List<String> liveNodes = _context.getProtocol().getLiveNodes();


        //遍历每个事件
        for (Iterator<OperationId> iter = _openOperationIds.iterator(); iter.hasNext(); ) {

            //事件ID
            OperationId operationId = iter.next();

            //如果Node中已经包含了这个事件, 也完成事件
            //检查如果所有活着的Node都没有要发布的这个Node名称, 事件白费了?
            if (!_context.getProtocol().isNodeOperationQueued(operationId) ||
                    !liveNodes.contains(operationId.getNodeName())) {
                iter.remove();
            }
        }
        if (isDone()) {
            //发布事件
            finishWatchdog();
        } else {
            LOG.debug("still " + getOpenOperationCount() + " open deploy operations");
        }
    }

    public synchronized void cancel() {
        _context.getProtocol().unregisterComponent(this);
        this.notifyAll();
    }


    /**
     * 发布事件
     */
    private synchronized void finishWatchdog() {
        InteractionProtocol protocol = _context.getProtocol();
        protocol.unregisterComponent(this);
        try {
            List<OperationResult> operationResults = new ArrayList<OperationResult>(_openOperationIds.size());

            //遍历所有的事件
            for (OperationId operationId : _operationIds) {

                //取得发布每一个事件的结果
                OperationResult operationResult = protocol.getNodeOperationResult(operationId, true);

                if (operationResult != null && operationResult.getUnhandledException() != null) {
                    LOG.error("received unhandlde exception from node " + operationId.getNodeName(), operationResult
                            .getUnhandledException());
                }
                operationResults.add(operationResult);// we add null ones
            }

            //完成该次事件,这里才是重要的
            _masterOperation.nodeOperationsComplete(_context, operationResults);
        } catch (Exception e) {
            LOG.info("operation complete action of " + _masterOperation + " failed", e);
        }
        LOG.info("watch for " + _masterOperation + " finished");
        this.notifyAll();
        _context.getMasterQueue().removeWatchdog(this);
    }

    public String getQueueElementId() {
        return _queueElementId;
    }

    public MasterOperation getOperation() {
        return _masterOperation;
    }

    public List<OperationId> getOperationIds() {
        return _operationIds;
    }

    public final int getOpenOperationCount() {
        return _openOperationIds.size();
    }

    public boolean isDone() {
        return _openOperationIds.isEmpty();
    }

    public final synchronized void join() throws InterruptedException {
        join(0);
    }

    public final synchronized void join(long timeout) throws InterruptedException {
        if (!isDone()) {
            this.wait(timeout);
        }
    }

    @Override
    public final void disconnect() {
        // handled by master
    }

    @Override
    public final void reconnect() {
        // handled by master
    }

}
