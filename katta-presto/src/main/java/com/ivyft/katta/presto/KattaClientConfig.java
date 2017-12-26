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

import io.airlift.configuration.Config;
import io.airlift.configuration.DefunctConfig;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@DefunctConfig("katta.connection")
public class KattaClientConfig {


    private String zookeeperServers = "localhost:2181";


    /**
     * 60S
     */
    private int connectionTimeout = 60_000;


    /**
     * 60s
     */
    private int sessionTimeout = 60_000;



    // query configurations
    private int batchSize = 5000; // use driver default



    @NotNull
    public String getZookeeperServers() {
        return zookeeperServers;
    }


    @Config("zookeeper.servers")
    public void setZookeeperServers(String zookeeperServers) {
        this.zookeeperServers = zookeeperServers;
    }


    @Min(0)
    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    @Config("zookeeper.connection.timeout")
    public KattaClientConfig setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    @Min(0)
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    @Config("zookeeper.session.timeout")
    public KattaClientConfig setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
        return this;
    }


    public int getBatchSize() {
        return batchSize;
    }

    @Config("katta.input.batch-size")
    public KattaClientConfig setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }


}
