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
package com.ivyft.katta.util;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;


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
public class ZkKattaUtil {


    /**
     * 默认的zk端口号
     */
    public static final int DEFAULT_PORT = 2181;


    /**
     * 创建一个zkClient链接
     *
     * @param conf 配置信息
     *
     * @param connectionTimeout 超时时间
     *
     * @return 返回ZkClient
     */
    public static ZkClient startZkClient(ZkConfiguration conf, int connectionTimeout) {
        return new ZkClient(conf.getZKServers(),
                conf.getZKTimeOut(),
                connectionTimeout);
    }


    /**
     * 使用默认端口号创建一个ZKServer
     * @param conf 配置信息
     * @return 返回创建的ZKServer
     */
    public static ZkServer startZkServer(ZkConfiguration conf) {
        return startZkServer(conf, DEFAULT_PORT);
    }


    /**
     * 启动ZkServer服务器。创建本地的Zk
     * @param conf 配置信息
     * @param port 端口号
     * @return
     */
    public static ZkServer startZkServer(ZkConfiguration conf, int port) {
        ZkServer zkServer = new ZkServer(
                conf.getZKDataDir(),
                conf.getZKDataLogDir(),
                new DefaultNameSpaceImpl(conf), port,
                conf.getZKTickTime());
        zkServer.start();
        return zkServer;
    }
}
