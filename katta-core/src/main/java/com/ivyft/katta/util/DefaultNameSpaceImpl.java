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

import com.ivyft.katta.util.ZkConfiguration.PathDef;
import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * <p>
 *     该类用于向ZooKeeper中创建节点。
 * </p>
 *
 * Implements the default name space in zookeeper for this katta instance.
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
 *
 * @author ZhenQin
 *
 */
public class DefaultNameSpaceImpl implements IDefaultNameSpace {


    /**
     * ZK 配置
     */
    private ZkConfiguration conf;


    /**
     * Log
     */
    private static final Logger LOG = LoggerFactory.getLogger(DefaultNameSpaceImpl.class);


    /**
     * 构造方法
     * @param conf ZK conf
     */
    public DefaultNameSpaceImpl(ZkConfiguration conf) {
        this.conf = conf;
    }


    @Override
    public void createDefaultNameSpace(ZkClient zkClient) {
        LOG.debug("Creating default File structure if required....");
        safeCreate(zkClient, this.conf.getZkRootPath());


        PathDef[] values = PathDef.values();
        for (PathDef pathDef : values) {
            if (pathDef != PathDef.MASTER && pathDef != PathDef.VERSION) {
                safeCreate(zkClient, this.conf.getZkPath(pathDef));
            }
        }
    }

    private void safeCreate(ZkClient zkClient, String path) {
        try {
            // first create parent directories
            String parent = ZkConfiguration.getZkParent(path);
            if (parent != null && !zkClient.exists(parent)) {
                //递归， 创建所有的节点
                safeCreate(zkClient, parent);
            }

            zkClient.createPersistent(path);
            LOG.info("created zookeeper node: " + path);
        } catch (ZkNodeExistsException e) {
            // Ignore if the node already exists.
        }
    }
}
