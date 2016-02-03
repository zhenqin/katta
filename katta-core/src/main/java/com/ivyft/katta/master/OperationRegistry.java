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

import com.ivyft.katta.operation.master.MasterOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
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
public class OperationRegistry {

    /**
     * Master上下文
     */
    private final MasterContext context;


    /**
     * 监控时间列表
     */
    private final List<OperationWatchdog> watchdogs = new ArrayList<OperationWatchdog>();


    /**
     * log
     */
    private final static Logger LOG = LoggerFactory.getLogger(OperationRegistry.class);


    /**
     * 默认构造方法
     * @param context
     */
    public OperationRegistry(MasterContext context) {
        this.context = context;
    }


    /**
     *
     * 完成一个事件上一次未完成的事件
     *
     *
     * @param watchdog 事件
     */
    public synchronized void watchFor(OperationWatchdog watchdog) {
        LOG.info("watch operation '" + watchdog.getOperation() + "' for node operations " + watchdog.getOperationIds());

        //清空上一次已经成功的事件
        releaseDoneWatchdogs();

         // 加入本次的事件
        watchdogs.add(watchdog);

        //开始执行
        watchdog.start(this.context);
    }


    /**
     * 移除已经成功的事件
     */
    private void releaseDoneWatchdogs() {
        for (Iterator<OperationWatchdog> iterator = watchdogs.iterator(); iterator.hasNext(); ) {
            OperationWatchdog watchdog = iterator.next();
            if (watchdog.isDone()) {
                this.context.getMasterQueue().removeWatchdog(watchdog);
                iterator.remove();
            }
        }
    }

    public synchronized List<MasterOperation> getRunningOperations() {
        List<MasterOperation> operations = new ArrayList<MasterOperation>();
        for (Iterator<OperationWatchdog> iterator = watchdogs.iterator(); iterator.hasNext(); ) {
            OperationWatchdog watchdog = iterator.next();
            if (watchdog.isDone()) {
                iterator.remove(); // lazy cleaning
            } else {
                operations.add(watchdog.getOperation());
            }
        }
        return operations;
    }

    public synchronized void shutdown() {
        for (Iterator<OperationWatchdog> iterator = watchdogs.iterator(); iterator.hasNext(); ) {
            OperationWatchdog watchdog = iterator.next();
            watchdog.cancel();
            iterator.remove();
        }
    }

}
