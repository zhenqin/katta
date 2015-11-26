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

import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.MasterQueue;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.util.HadoopUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


/**
 * 该类保存了Master的上下文信息，重要的变量。
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
public class MasterContext {

    /**
     * Master实例
     */
    private final Master master;


    /**
     * 操作ZooKeeper便捷类
     */
    private final InteractionProtocol protocol;


    /**
     * 部署和分发
     */
    private final IDeployPolicy deployPolicy;


    /**
     * 事件队列
     */
    private final MasterQueue masterQueue;


    /**
     *
     * @param protocol
     * @param master
     * @param deployPolicy
     * @param masterQueue
     */
    public MasterContext(InteractionProtocol protocol, Master master, IDeployPolicy deployPolicy, MasterQueue masterQueue) {
        this.protocol = protocol;
        this.master = master;
        this.deployPolicy = deployPolicy;
        this.masterQueue = masterQueue;
    }

    public InteractionProtocol getProtocol() {
        return this.protocol;
    }

    public Master getMaster() {
        return this.master;
    }

    public IDeployPolicy getDeployPolicy() {
        return this.deployPolicy;
    }

    public MasterQueue getMasterQueue() {
        return this.masterQueue;
    }

    public FileSystem getFileSystem(IndexMetaData indexMd) throws IOException {
        return HadoopUtil.getFileSystem(new Path(indexMd.getPath()));
    }

}
