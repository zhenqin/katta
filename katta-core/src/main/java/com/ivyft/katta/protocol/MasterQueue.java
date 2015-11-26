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
package com.ivyft.katta.protocol;

import com.ivyft.katta.master.OperationWatchdog;
import com.ivyft.katta.operation.OperationId;
import com.ivyft.katta.operation.master.MasterOperation;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

import java.util.ArrayList;
import java.util.List;



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
public class MasterQueue extends BlockingQueue<MasterOperation> {


    /**
     * "/katta/watchdogs"节点
     *
     */
    private String _watchdogsPath;


    /**
     * 构造方法
     * @param zkClient
     * @param rootPath
     */
    public MasterQueue(ZkClient zkClient, String rootPath) {
        super(zkClient, rootPath);

        //给看门狗设置一个,相当一发布一个事件
        _watchdogsPath = rootPath + "/watchdogs";
        zkClient.createPersistent(_watchdogsPath, true);

        //给看门狗设置一个,相当一发布一个事件./katta/work/master-queue/watchdogs
        List<String> watchdogs = zkClient.getChildren(_watchdogsPath);

        //遍历看门狗的子节点
        for (String elementName : watchdogs) {
            try {
                //移除旧的主节点。 可能是上一次Master退出没有删除主节点
                zkClient.delete(getElementPath(elementName));
            } catch (ZkNoNodeException e) {
                // ignore, can be already deleted by other queue instance
            }
        }
    }



    private String getWatchdogPath(String elementId) {
        return _watchdogsPath + "/" + elementId;
    }

    /**
     * Moves the top of the queue to the watching state.
     *
     * @param masterOperation
     * @param nodeOperationIds
     * @return
     * @throws InterruptedException
     */
    public OperationWatchdog moveOperationToWatching(MasterOperation masterOperation,
                                                     List<OperationId> nodeOperationIds)
            throws InterruptedException {

        Element<MasterOperation> element = getFirstElement();
        // we don't use the persisted operation cause the given masterOperation can
        // have a changed state
        OperationWatchdog watchdog = new OperationWatchdog(element.getName(), masterOperation, nodeOperationIds);


        zkClient.createPersistent(getWatchdogPath(element.getName()), watchdog);
        zkClient.delete(getElementPath(element.getName()));
        return watchdog;
    }


    /**
     * 读取看门狗下发布的所有事件.
     * @return 返回看门狗下发布的所有事件.
     */
    public List<OperationWatchdog> getWatchdogs() {
        List<String> childs = zkClient.getChildren(_watchdogsPath);
        List<OperationWatchdog> watchdogs = new ArrayList<OperationWatchdog>(childs.size());
        for (String child : childs) {
            watchdogs.add((OperationWatchdog) zkClient.readData(getWatchdogPath(child)));
        }
        return watchdogs;
    }


    /**
     * 删除看门狗下的一个事件.
     * @param watchdog 事件类型,其中包含了ID
     */
    public void removeWatchdog(OperationWatchdog watchdog) {
        zkClient.delete(getWatchdogPath(watchdog.getQueueElementId()));
    }

}
