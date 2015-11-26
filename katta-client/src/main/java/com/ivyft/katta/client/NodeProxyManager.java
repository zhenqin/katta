/**
 * Copyright 2011 the original author or authors.
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
package com.ivyft.katta.client;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.ivyft.katta.lib.lucene.ILuceneServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 *
 * <p>
 *     该类是一个远程RPC Proxy缓存类, 主要缓存已创建的RPC连接.
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
public class NodeProxyManager implements INodeProxyManager {


    /**
     * RPC 协议
     */
    protected final Class<? extends VersionedProtocol> serverClass;


    /**
     * Hadoop Configuration
     */
    protected final Configuration hadoopConf;


    /**
     * 缓存的Map, 直接放到内存中,node->RPC的方式
     */
    protected final Map<String, VersionedProtocol> node2ProxyMap = new ConcurrentHashMap<String, VersionedProtocol>();


    /**
     * 访问授权
     */
    protected final INodeSelectionPolicy selectionPolicy;


    private int successiveProxyFailuresBeforeReestablishing = 3;


    private final Multiset<String> failedNodeInteractions = HashMultiset.create();


    /**
     * Log
     */
    private final static Logger log = LoggerFactory.getLogger(NodeProxyManager.class);

    /**
     *
     * @param serverClass
     * @param hadoopConf
     * @param selectionPolicy
     */
    public NodeProxyManager(Class<? extends VersionedProtocol> serverClass, Configuration hadoopConf,
                            INodeSelectionPolicy selectionPolicy) {
        this.serverClass = serverClass;
        this.hadoopConf = hadoopConf;
        this.selectionPolicy = selectionPolicy;
    }


    /**
     * 根据Node创建一个连接, 也可能从缓存中取出连接, 取决于establishIfNoExists参数
     * @param nodeName Node节点
     * @param establishIfNoExists establishIfNoExists=true,则相当启用缓存
     * @return 返回创建的连接
     */
    @Override
    public VersionedProtocol getProxy(String nodeName, boolean establishIfNoExists) {
        VersionedProtocol versionedProtocol = node2ProxyMap.get(nodeName);
        if (versionedProtocol == null && establishIfNoExists) {
            synchronized (nodeName.intern()) {
                if (!node2ProxyMap.containsKey(nodeName)) {
                    try {
                        versionedProtocol = createNodeProxy(nodeName);
                        node2ProxyMap.put(nodeName, versionedProtocol);
                    } catch (Exception e) {
                        log.warn("Could not create proxy for node '" + nodeName + "' - " + e.getClass().getSimpleName() + ": "
                                + e.getMessage());
                    }
                }
            }
        }
        return versionedProtocol;
    }

    @Override
    public Map<String, List<String>> createNode2ShardsMap(Collection<String> shards) throws ShardAccessException {
        return selectionPolicy.createNode2ShardsMap(shards);
    }

    @Override
    public void reportNodeCommunicationFailure(String nodeName, Throwable t) {
        // re-establishing it would fix the communication. If so, we should check
        // the for the exception which occurs in such cases and re-establish the
        // proxy.
        failedNodeInteractions.add(nodeName);
        int failureCount = failedNodeInteractions.count(nodeName);
        if (failureCount >= successiveProxyFailuresBeforeReestablishing
                || exceptionContains(t, ConnectException.class, EOFException.class)) {
            dropNodeProxy(nodeName, failureCount);
        }
    }




    @Override
    public void reportNodeCommunicationSuccess(String node) {
        failedNodeInteractions.remove(node, Integer.MAX_VALUE);
    }



    @Override
    public void shutdown() {
        Collection<VersionedProtocol> proxies = node2ProxyMap.values();
        for (VersionedProtocol search : proxies) {
            RPC.stopProxy(search);
        }
    }


    /**
     * 取得NodeName上的RPC连接
     * @param nodeName Node
     * @return
     * @throws IOException
     */
    public VersionedProtocol createNodeProxy(String nodeName) throws IOException {
        log.info("creating proxy for node: " + nodeName);

        String[] hostName_port = nodeName.split(":");
        if (hostName_port.length != 2) {
            throw new RuntimeException("invalid node name format '" + nodeName
                    + "' (It should be a host name with a port number devided by a ':')");
        }
        String hostName = hostName_port[0];
        String port = hostName_port[1];

        InetSocketAddress inetSocketAddress =
                new InetSocketAddress(hostName, Integer.parseInt(port));
        VersionedProtocol proxy = RPC.getProxy(serverClass,
                ILuceneServer.versionID,
                inetSocketAddress,
                hadoopConf);

        if(log.isDebugEnabled()) {
            log.debug(String.format("Created a proxy %s for %s:%s %s", Proxy.getInvocationHandler(proxy), hostName, port,
                    inetSocketAddress));
        }
        return proxy;
    }


    private boolean exceptionContains(Throwable t, Class<? extends Throwable>... exceptionClasses) {
        while (t != null) {
            for (Class<? extends Throwable> exceptionClass : exceptionClasses) {
                if (t.getClass().equals(exceptionClass)) {
                    return true;
                }
            }
            t = t.getCause();
        }
        return false;
    }

    private void dropNodeProxy(String nodeName, int failureCount) {
        synchronized (nodeName.intern()) {
            if (node2ProxyMap.containsKey(nodeName)) {
                log.warn("removing proxy for node '" + nodeName + "' after " + failureCount + " proxy-invocation errors");
                failedNodeInteractions.remove(nodeName, Integer.MAX_VALUE);
                selectionPolicy.removeNode(nodeName);
                VersionedProtocol proxy = node2ProxyMap.remove(nodeName);
                RPC.stopProxy(proxy);
            }
        }
    }


    /**
     * @return how many successive proxy invocation errors must happen before the
     *         proxy is re-established.
     */
    public int getSuccessiveProxyFailuresBeforeReestablishing() {
        return successiveProxyFailuresBeforeReestablishing;
    }

    public void setSuccessiveProxyFailuresBeforeReestablishing(int successiveProxyFailuresBeforeReestablishing) {
        this.successiveProxyFailuresBeforeReestablishing = successiveProxyFailuresBeforeReestablishing;
    }

}
