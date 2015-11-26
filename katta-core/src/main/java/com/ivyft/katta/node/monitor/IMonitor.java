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


import com.ivyft.katta.protocol.InteractionProtocol;

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
public interface IMonitor {


    /**
     * 开始监控。 并把监控信息写入ZooKeeper
     * @param nodeId startID， 生成的
     * @param protocol Node Protocol
     */
    void startMonitoring(String nodeId, InteractionProtocol protocol);


    /**
     * 停止监控
     */
    public void stopMonitoring();



}
