/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ivyft.katta.presto;

import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.*;
import com.ivyft.katta.client.INodeProxyManager;
import com.ivyft.katta.client.NodeProxyManager;
import com.ivyft.katta.client.ShuffleNodeSelectionPolicy;
import com.ivyft.katta.lib.lucene.ILuceneServer;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.HadoopUtil;
import com.ivyft.katta.util.ZkConfiguration;
import io.airlift.log.Logger;
import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Singleton;

import java.util.Properties;

import io.airlift.configuration.ConfigBinder;
import static java.util.Objects.requireNonNull;

public class KattaClientModule implements Module {


    private static final Logger LOGGER = Logger.get(KattaClientModule.class);


    @Override
    public void configure(Binder binder) {
        binder.bind(KattaConnector.class).in(Scopes.SINGLETON);
        binder.bind(KattaSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(KattaPageSourceProvider.class).in(Scopes.SINGLETON);


        ConfigBinder.configBinder(binder).bindConfig(KattaClientConfig.class);
    }


    @Singleton
    @Provides
    public static Configuration createHadoopConf(KattaClientConfig config) {
        Configuration hadoopConf = HadoopUtil.getHadoopConf();
        //hadoopConf.set("hadoop.rpc.socket.factory.class.default", org.apache.commons.net.DefaultSocketFactory.class.getName());
        hadoopConf.set("hadoop.rpc.socket.factory.class.default", KattaSocketFactory.class.getName());
        hadoopConf.setInt("katta.input.batch-size", config.getBatchSize());
        hadoopConf.setInt("katta.input.limit", config.getBatchSize());
        hadoopConf.set("zookeeper.servers", config.getZookeeperServers());
        return hadoopConf;
    }


    @Singleton
    @Provides
    public static InteractionProtocol createKattaProtocol(KattaClientConfig config) {
        ZkClient zkClient = new ZkClient(config.getZookeeperServers(),
                config.getSessionTimeout(),
                config.getConnectionTimeout());

        Properties properties = new Properties();
        properties.setProperty(ZkConfiguration.DEFAULT_ROOT_PATH, "/katta");
        properties.setProperty(ZkConfiguration.ZOOKEEPER_EMBEDDED, "false");
        properties.setProperty(ZkConfiguration.ZOOKEEPER_SERVERS, config.getZookeeperServers());
        properties.setProperty(ZkConfiguration.ZOOKEEPER_TIMEOUT, config.getConnectionTimeout()+"");
        properties.setProperty(ZkConfiguration.ZOOKEEPER_TICK_TIME, config.getSessionTimeout() + "");
        ZkConfiguration zkConf = new ZkConfiguration(properties, null);
        return new InteractionProtocol(zkClient, zkConf);
    }


    @Singleton
    @Provides
    public static INodeProxyManager createNodeProxyManager(Configuration conf) {
        INodeProxyManager proxyManager = new NodeProxyManager<>(ILuceneServer.class,
                conf, new ShuffleNodeSelectionPolicy());

        proxyManager.getProxy("localhost:20000", true);
        return proxyManager;
    }


    @Singleton
    @Provides
    public static KattaSession createKattaSession(TypeManager typeManager,
                                                  InteractionProtocol protocol,
                                                  INodeProxyManager manager) {
        return new KattaSession(typeManager, protocol, manager);
    }

}
