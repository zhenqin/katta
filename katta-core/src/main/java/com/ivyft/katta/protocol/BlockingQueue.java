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

import org.I0Itec.zkclient.ExceptionUtil;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
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
public class BlockingQueue<T extends Serializable> {

    protected static class Element<T> {
        private String name;
        private T data;

        public Element(String name, T data) {
            this.name = name;
            this.data = data;
        }

        public String getName() {
            return name;
        }

        public T getData() {
            return data;
        }


        @Override
        public String toString() {
            return "Element{" +
                    "name='" + name + '\'' +
                    ", data=" + data +
                    '}';
        }
    }

    protected final ZkClient zkClient;


    /**
     * /katta/operations
     */
    private final String elementsPath;


    /**
     * Log
     */
    private static Logger log = LoggerFactory.getLogger(BlockingQueue.class);

    /**
     * constructor
     * @param zkClient
     * @param rootPath
     */
    public BlockingQueue(ZkClient zkClient, String rootPath) {
        this.zkClient = zkClient;

        // 目录这时为:/katta/work/master-queue/operations
        this.elementsPath = rootPath + "/operations";

        //创建父目录
        this.zkClient.createPersistent(rootPath, true);

        //创建/katta/work/master-queue/operations
        this.zkClient.createPersistent(this.elementsPath, true);
    }


    /**
     * 发布地址?
     *
     * @return 子节点事件
     */
    private String getElementRoughPath() {
        //发布地址? 这里可能有Bug
        return getElementPath("operation" + "-");
    }



    /**
     *
     *  返回"/katta/work/operations/elementId" 的节点
     *
     *
     * @param elementId elementId
     * @return 返回"/kattawork//operations/elementId" 的节点
     */
    public String getElementPath(String elementId) {

        ///katta/work/master-queue/operations-355
        return this.elementsPath + "/" + elementId;
    }



    /**
     *
     * 创建一个 master-queue 事件子节点
     *
     *
     * @param element
     * @return the id of the element in the queue
     */
    public String add(T element) {
        try {
            //发布了一个AbstractIndexOperation到 /katta/work/operations/operation-
            String sequential = this.zkClient.createPersistentSequential(getElementRoughPath(), element);
            String elementId = sequential.substring(sequential.lastIndexOf('/') + 1);
            log.info("add element: {}, data: {}", elementId, element.toString());
            return elementId;
        } catch (Exception e) {
            throw ExceptionUtil.convertToRuntimeException(e);
        }
    }


    /**
     * 删除第一个子节点事件
     * @return
     * @throws InterruptedException
     */
    public T remove() throws InterruptedException {
        Element<T> element = getFirstElement();
        log.info("remove {}", element.toString());
        this.zkClient.delete(getElementPath(element.getName()));
        return element.getData();
    }


    /**
     * 是否包含该子节点
     *
     * @param elementId
     * @return
     */
    public boolean containsElement(String elementId) {
        String zkPath = getElementPath(elementId);
        return this.zkClient.exists(zkPath);
    }


    /**
     * 如果没有数据，调用该方法会一直死锁，知道从ZK中读得数据
     * @return 从ZK中读得数据，一般都是一些Master分发的命令
     * @throws InterruptedException
     */
    public T peek() throws InterruptedException {
        Element<T> element = getFirstElement();
        if (element == null) {
            return null;
        }
        return element.getData();
    }


    /**
     * 返回 Master Queue Children Size
     * @return 返回子节点个数
     */
    public int size() {
        return this.zkClient.getChildren(this.elementsPath).size();
    }


    /**
     * master-queue 子节点 size
     * @return 返回时子节点 size 是否等于 0
     */
    public boolean isEmpty() {
        return size() == 0;
    }


    /**
     * 获取消息列表中, 最新的消息
     * @param list 消息
     * @return 返回最近的一个
     */
    private String getSmallestElement(List<String> list) {
        String smallestElement = list.get(0);
        for (String element : list) {
            if (element.compareTo(smallestElement) < 0) {
                smallestElement = element;
            }
        }

        return smallestElement;
    }


    /**
     * 调用该方法会死锁
     * @return 直到返回数据
     * @throws InterruptedException
     */
    protected Element<T> getFirstElement() throws InterruptedException {
        final Object mutex = new Object();

        //TODO Master 获取集群的事件
        IZkChildListener notifyListener = new IZkChildListener() {
            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                log.info(parentPath + " has node changed, children: " + currentChilds);
                synchronized (mutex) {
                    //有新的节点创建，或者删除。立即激活
                    mutex.notify();
                }
            }
        };
        try {
            while (true) {
                List<String> elementNames;
                synchronized (mutex) {
                    log.info("listening zk: " + elementsPath);
                    elementNames = this.zkClient.subscribeChildChanges(this.elementsPath, notifyListener);
                    while (elementNames == null || elementNames.isEmpty()) {
                        mutex.wait();

                        //得到节点下的所有节点
                        elementNames = this.zkClient.getChildren(this.elementsPath);
                    }
                }

                if(elementNames == null || elementNames.isEmpty()) {
                    continue;
                }

                //得到新的节点名称
                String elementName = getSmallestElement(elementNames);
                try {
                    //以新节点名称创建一个Path
                    String elementPath = getElementPath(elementName);

                    //读出Path的数据信息，放在data中，data中是一个对象。
                    Element<T> element = new Element<T>(elementName, (T) this.zkClient.readData(elementPath));
                    log.info("elementName: {}, elementPath: {}, data: {}", elementName, elementPath, element.toString());
                    return element;
                } catch (ZkNoNodeException e) {
                    // somebody else picked up the element first, so we have to
                    // retry with the new first element
                }
            }
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw ExceptionUtil.convertToRuntimeException(e);
        } finally {
            this.zkClient.unsubscribeChildChanges(this.elementsPath, notifyListener);
        }
    }

}
