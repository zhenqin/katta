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
import com.ivyft.katta.operation.master.MasterOperation.ExecutionInstruction;
import com.ivyft.katta.protocol.MasterQueue;
import org.I0Itec.zkclient.ExceptionUtil;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 *
 * <p>
 *   Responsible for executing all {@link MasterOperation}s which are offered to
 * the master queue sequentially.
 * </p>
 *
 * <p>
 *     添加节点,添加shard事件的搬运工.
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
class OperatorThread extends Thread {


    /**
     * 上下文对象。保存了Master可操作的重要的实例句柄
     */
    private final MasterContext _context;


    /**
     * 注册的Master队列
     */
    private final MasterQueue _queue;

    /**
     * 注册服务
     */
    private final OperationRegistry _registry;


    /**
     * Master保持safeMode时间. 安全模式是Master发现Node的时间
     */
    private final long safeModeMaxTime;


    /**
     * 当前状态是否是safeMode？
     */
    private boolean safeMode = true;

    /**
     * log
     */
    protected final static Logger LOG = LoggerFactory.getLogger(OperatorThread.class);


    /**
     * 构造方法
     *
     * @param context
     * @param safeModeMaxTime
     */
    public OperatorThread(final MasterContext context, final long safeModeMaxTime) {
        _context = context;
        _queue = context.getMasterQueue();
        _registry = new OperationRegistry(context);
        this.setDaemon(true);
        this.setName(getClass().getSimpleName());
        this.safeModeMaxTime = safeModeMaxTime;
    }

    @Override
    public void run() {
        try {
            LOG.info("starting master operator thread...");

            //在安全模式内发现Node的成员
            runInSafeMode();

            //完成上一次未完成的事件
            recreateWatchdogs();

            while (true) {
                try {
                    //TODO Master 全局的操作
                    MasterOperation operation = _queue.peek();
                    List<OperationId> nodeOperationIds = null;
                    try {
                        List<MasterOperation> runningOperations = _registry.getRunningOperations();
                        ExecutionInstruction instruction = operation.getExecutionInstruction(runningOperations);

                        //发布事件，使Node执行
                        nodeOperationIds = executeOperation(operation, instruction, runningOperations);
                    } catch (Exception e) {
                        ExceptionUtil.rethrowInterruptedException(e);
                        LOG.error("failed to execute " + operation, e);
                    }
                    if (nodeOperationIds != null && !nodeOperationIds.isEmpty()) {
                        OperationWatchdog watchdog = _queue.moveOperationToWatching(operation, nodeOperationIds);

                        //发布一个事件，立即监听各个Node的执行结果
                        _registry.watchFor(watchdog);
                    } else {
                        _queue.remove();
                    }
                } catch (InterruptedException e) {
                    break;
                    // let go the thread
                } catch (ZkInterruptedException e){
                    LOG.error(ExceptionUtils.getFullStackTrace(e));
                    break;
                } catch (Throwable e) {
                    LOG.error(ExceptionUtils.getFullStackTrace(e));
                }
            }
        } catch (final InterruptedException e) {

        } catch (ZkInterruptedException e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            // let go the thread
        }
        _registry.shutdown();
        LOG.info("operator thread stopped");
    }


    /**
     * 激活看门狗, 用于发布事件
     */
    private void recreateWatchdogs() {
        //取得看门狗下的所有事件
        List<OperationWatchdog> watchdogs = _context.getMasterQueue().getWatchdogs();
        for (OperationWatchdog watchdog : watchdogs) {
            //把没有完成的继续
            if (watchdog.isDone()) {
                LOG.info("release done watchdog " + watchdog);
                //完成了的事件删除
                _queue.removeWatchdog(watchdog);
            } else {
                //没有完成的, 继续
                _registry.watchFor(watchdog);
            }
        }
    }

    private List<OperationId> executeOperation(MasterOperation operation,
                                               ExecutionInstruction instruction,
                                               List<MasterOperation> runningOperations) throws Exception {
        List<OperationId> operationIds = null;
        switch (instruction) {
            case EXECUTE:
                LOG.info("executing operation '" + operation + "'");
                operationIds = operation.execute(_context, runningOperations);
                break;
            case CANCEL:
                // just do nothing
                LOG.info("skipping operation '" + operation + "'");
                break;
            case ADD_TO_QUEUE_TAIL:
                LOG.info("adding operation '" + operation + "' to end of queue");
                _queue.add(operation);
                break;
            default:
                throw new IllegalStateException("execution instruction " + instruction + " not handled");
        }
        return operationIds;
    }


    /**
     * 在启动初期安全模式下的操作
     * @throws InterruptedException
     */
    private void runInSafeMode() throws InterruptedException {
        this.safeMode = true;
        // List<String> knownNodes = _protocol.getKnownNodes();
        // 活着的Node ?
        List<String> previousLiveNodes = _context.getProtocol().getLiveNodes();

        //记录事件
        long lastChange = System.currentTimeMillis();
        try {
            //等待Node上线, 或者等待到了最大时间
            while (previousLiveNodes.isEmpty() || lastChange + this.safeModeMaxTime > System.currentTimeMillis()) {
                LOG.info("SAFE MODE: No nodes available or state unstable within the last " + this.safeModeMaxTime + " ms.");
                Thread.sleep(this.safeModeMaxTime / 4);

                //获得Node成员
                List<String> currentLiveNodes = _context.getProtocol().getLiveNodes();
                if (currentLiveNodes.size() != previousLiveNodes.size()) {
                    lastChange = System.currentTimeMillis();
                    previousLiveNodes = currentLiveNodes;
                }
            }
            LOG.info("SAFE MODE: leaving safe mode with " + previousLiveNodes.size() + " connected nodes");
        } finally {
            this.safeMode = false;
        }
    }


    public boolean isInSafeMode() {
        return this.safeMode;
    }

    public OperationRegistry getOperationRegistry() {
        return _registry;
    }

    public MasterContext getContext() {
        return _context;
    }

}
