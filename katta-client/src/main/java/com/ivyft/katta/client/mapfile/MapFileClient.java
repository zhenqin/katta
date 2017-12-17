/**
 * Copyright 2009 the original author or authors.
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
package com.ivyft.katta.client.mapfile;

import com.ivyft.katta.client.Client;
import com.ivyft.katta.client.INodeSelectionPolicy;
import com.ivyft.katta.client.IResultReceiver;
import com.ivyft.katta.lib.mapfile.IMapFileServer;
import com.ivyft.katta.client.ClientConfiguration;
import com.ivyft.katta.util.KattaException;
import com.ivyft.katta.util.ZkConfiguration;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;

/**
 * The front end to the MapFile server.
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
public class MapFileClient implements IMapFileClient {



    private static final Method GET_METHOD;
    private static final int GET_METHOD_SHARD_ARG_IDX = 1;

    static {
        try {
            GET_METHOD = IMapFileServer.class.getMethod("get",
                    new Class[]{Text.class, String[].class});
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method get() in IMapFileServer!");
        }
    }


    private static final long TIMEOUT = 12000;

    private Client kattaClient;


    private static final Logger LOG = LoggerFactory.getLogger(MapFileClient.class);


    /**
     * default constructor
     *
     */
    public MapFileClient() {
        kattaClient = new Client(IMapFileServer.class);
    }


    public MapFileClient(final INodeSelectionPolicy nodeSelectionPolicy) {
        kattaClient = new Client(IMapFileServer.class, nodeSelectionPolicy);
    }


    public MapFileClient(final ZkConfiguration zkConfig) {
        kattaClient = new Client(IMapFileServer.class, zkConfig);
    }

    public MapFileClient(final INodeSelectionPolicy policy, final ZkConfiguration czkCnfig) {
        kattaClient = new Client(IMapFileServer.class, policy, czkCnfig);
    }

    public MapFileClient(final INodeSelectionPolicy policy, final ZkConfiguration zkConfig,
                         ClientConfiguration clientConfiguration) {
        kattaClient = new Client(IMapFileServer.class, policy, zkConfig, clientConfiguration);
    }


    public List<String> get(final String key,
                            final String[] indexNames) throws KattaException {
        IResultReceiver<List<String>> results =
                kattaClient.broadcastToIndices(TIMEOUT,
                        true,
                        GET_METHOD,
                        GET_METHOD_SHARD_ARG_IDX,
                        indexNames,
                        new Text(key),
                        null);
        if (results.isError()) {
            throw results.getKattaException();
        }
        return results.getResult();
    }


    public void close() {
        kattaClient.close();
    }

}
