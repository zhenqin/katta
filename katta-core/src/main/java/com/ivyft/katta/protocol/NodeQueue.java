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

import com.ivyft.katta.operation.node.NodeOperation;
import com.ivyft.katta.operation.node.OperationResult;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
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
public class NodeQueue extends BlockingQueue<NodeOperation> {


    /**
     * 结果写在那个地址上?
     */
    private String resultsPath;


    /**
     * 一个部署的结果路径,发布事件使用
     * @param zkClient
     * @param rootPath foot
     */
    public NodeQueue(ZkClient zkClient, String rootPath) {
        super(zkClient, rootPath);
        this.resultsPath = rootPath + "/results";
        zkClient.createPersistent(this.resultsPath, true);

        // cleanup odd result situations
        List<String> results = zkClient.getChildren(this.resultsPath);
        for (String elementName : results) {
            try {
                zkClient.delete(getElementPath(elementName));
            } catch (ZkNoNodeException e) {
                // ignore, can be already deleted by other queue instance
            }
        }
    }

    private String getResultPath(String elementId) {
        //DIR: /katta/nodes/node-queues/results/elementId
        return this.resultsPath + "/" + elementId;
    }



    public String add(NodeOperation element) {
        String elementName = super.add(element);
        zkClient.delete(getResultPath(elementName));
        return elementName;
    }


    /**
     * 把Master分发给Node的事件执行的结果写到ZooKeeper中，提交结果
     * @param result 执行结果
     * @return 返回执行数据
     * @throws InterruptedException
     */
    public NodeOperation complete(OperationResult result) throws InterruptedException {
        Element<NodeOperation> element = getFirstElement();
        if (result != null) {

            //DIR: /katta/nodes/node-queues/results/elementId
            zkClient.createEphemeral(getResultPath(element.getName()), result);
        }

        ////katta/work/master-queue/operations/elementId
        zkClient.delete(getElementPath(element.getName()));
        return element.getData();
    }


    /**
     * 获得Node执行时间的结果
     * @param elementId
     * @param remove
     * @return
     */
    public Serializable getResult(String elementId, boolean remove) {
        ///katta/nodes/node-queues/results/elementId
        String zkPath = getResultPath(elementId);
        Serializable result = zkClient.readData(zkPath, true);
        if (remove) {
            zkClient.delete(zkPath);
        }
        return result;
    }


    /**
     * 获得Node执行的所有的事件的结果
     * @return
     */
    public List<OperationResult> getResults() {
        List<String> childs = zkClient.getChildren(this.resultsPath);
        List<OperationResult> watchdogs = new ArrayList<OperationResult>(childs.size());
        for (String child : childs) {
            watchdogs.add((OperationResult) zkClient.readData(getResultPath(child)));
        }
        return watchdogs;
    }

}
