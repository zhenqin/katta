/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.ivyft.katta.yarn;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.ivyft.katta.lib.lucene.FreeSocketPortFactory;
import com.ivyft.katta.lib.lucene.SocketPortFactory;
import com.ivyft.katta.util.KattaConfiguration;
import com.ivyft.katta.util.NetworkUtils;
import com.ivyft.katta.yarn.protocol.KattaYarnAvroServer;
import com.ivyft.katta.yarn.protocol.KattaYarnMasterProtocol;
import com.ivyft.katta.yarn.protocol.KattaYarnProtocol;
import org.apache.avro.AvroRemoteException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/28
 * Time: 19:51
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaAppMaster implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(KattaAppMaster.class);

    private final KattaYarnAvroServer server;


    private final KattaConfiguration conf;

    private final KattaAMRMClient client;


    private final KattaYarnMasterProtocol protocol;

    private final BlockingQueue<Object> launcherQueue = new LinkedBlockingQueue<Object>();


    public KattaAppMaster(KattaConfiguration conf,
                          KattaAMRMClient client) {
        this(conf, client, new KattaYarnMasterProtocol(conf, client));
    }

    private KattaAppMaster(KattaConfiguration conf,
                           KattaAMRMClient client,
                           KattaYarnMasterProtocol protocol) {
        this.client = client;
        this.conf = conf;
        this.protocol = protocol;

        this.server = new KattaYarnAvroServer(KattaYarnProtocol.class, protocol);
        int port = conf.getInt(KattaOnYarn.MASTER_AVRO_PORT, 4880);

        SocketPortFactory portFactory = new FreeSocketPortFactory();
        port = portFactory.getSocketPort(port, conf.getInt(KattaOnYarn.MASTER_AVRO_PORT + ".step", 1));
        this.server.setHost(NetworkUtils.getLocalhostName());
        //this.server.setHost("zhenqin-pro102");
        this.server.setPort(port);

        LOG.info("katta application master start at: " + getAvroServerHost() + ":" + getAvroServerPort());

        //把使用的端口重新设置, 这样是防止在同一个 Node 上运行多个 AppMaster
        conf.setProperty(KattaOnYarn.MASTER_AVRO_PORT, port);

        protocol.setAppMaster(this);
        try {
            LOG.info("launch katta master");
            //protocol.startMaster(1);

            int numKattaNode =
                    conf.getInt(KattaOnYarn.DEFAULT_KATTA_NODE_NUM, 1);
            LOG.info("launch katta node, node num: " + numKattaNode);
            protocol.startNode(numKattaNode);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void serve() {
        this.server.serve();
    }


    public void stop() {
        if (server != null) {
            try {
                server.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            protocol.stopMaster();
        } catch (AvroRemoteException e) {
            e.printStackTrace();
        }

        try {
            protocol.stopAllNode();
        } catch (AvroRemoteException e) {
            e.printStackTrace();
        }
    }


    public void add(Object o) {
        launcherQueue.add(o);
    }

    @Override
    public void run() {
        try {
            int heartBeatIntervalMs = conf.getInt(KattaOnYarn.MASTER_HEARTBEAT_INTERVAL_MILLIS, 10000);
            int count = 10;

            while (client.getServiceState() == Service.STATE.STARTED &&
                    !Thread.currentThread().isInterrupted()) {

                // We always send 50% progress.
                AllocateResponse allocResponse = client.allocate(0.5f);

                AMCommand am_command = allocResponse.getAMCommand();
                if (am_command != null &&
                        (am_command == AMCommand.AM_SHUTDOWN || am_command == AMCommand.AM_RESYNC)) {
                    LOG.info("Got AM_SHUTDOWN or AM_RESYNC from the RM");
                    return;
                }

                //取得 Yarn 还剩余的Container资源, Container代表可运行的进程
                List<Container> allocatedContainers = allocResponse.getAllocatedContainers();
                // Add newly allocated containers to the client.
                LOG.info("HB: Received allocated containers (" + allocatedContainers.size() + ")");

                if (allocatedContainers.size() > 0) {
                    //有资源? 等于0说明没资源了
                    client.addAllocatedContainers(allocatedContainers);
                }
                count++;
                LOG.info("counter: " + count);

                Thread.sleep(heartBeatIntervalMs);

                List<ContainerStatus> completedContainers =
                        allocResponse.getCompletedContainersStatuses();
                LOG.info(completedContainers.toString());
                LOG.info("HB: Containers completed (" + completedContainers.size() + "), so releasing them.");
            }
        } catch (Throwable t) {
            // Something happened we could not handle.  Make sure the AM goes
            // down so that we are not surprised later on that our heart
            // stopped..
            LOG.error("Unhandled error in AM: ", t);
            server.stop();
            System.exit(1);
        }
    }

    private void initAndStartLauncher() {
        Thread thread = new Thread() {
            @Override
            public void run() {
                while (client.getServiceState() == Service.STATE.STARTED &&
                        !Thread.currentThread().isInterrupted()) {
                    try {
                        Object take = launcherQueue.take();
                        if(take instanceof Shutdown) {
                            Thread.sleep(3000);
                            KattaAppMaster.this.stop();
                            continue;
                        } else if(take instanceof Container) {
                            Container container = (Container) take;
                            LOG.info("LAUNCHER: Taking container with id (" + container.getId() + ") from the queue.");
                        }
                    } catch (InterruptedException e) {
                        if (client.getServiceState() == Service.STATE.STARTED) {
                            LOG.error("Launcher thread interrupted : ", e);
                            System.exit(1);
                        }
                        return;
                    } catch (Exception e) {
                        LOG.error("Launcher thread I/O exception : ", e);
                        System.exit(1);
                    }
                }
            }
        };
        thread.setDaemon(true);
        thread.setName("katta-app-master-initQueue");
        thread.start();
    }


    private Thread initAndStartHeartbeat() {
        Thread thread = new Thread(this);
        thread.setDaemon(true);
        thread.setName("katta-app-master-heartbeat");
        thread.start();
        return thread;
    }


    public String getAvroServerHost() {
        return this.server.getHost();
    }

    public int getAvroServerPort() {
        return this.server.getPort();
    }



    public static void main(String[] args) throws Exception {
        LOG.info("Starting the AM!!!!");

        Options opts = new Options();
        opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used " +
                "unless for testing purposes");

        CommandLine cl = new GnuParser().parse(opts, args);

        ApplicationAttemptId appAttemptID;
        Map<String, String> envs = System.getenv();
        if (cl.hasOption("app_attempt_id")) {
            String appIdStr = cl.getOptionValue("app_attempt_id", "");
            appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
        } else if (envs.containsKey(ApplicationConstants.Environment.CONTAINER_ID.name())) {
            ContainerId containerId = ConverterUtils.toContainerId(envs
                    .get(ApplicationConstants.Environment.CONTAINER_ID.name()));
            appAttemptID = containerId.getApplicationAttemptId();
            LOG.info("appAttemptID from env:" + appAttemptID.toString());
        } else {
            LOG.error("appAttemptID is not specified for storm master");
            throw new Exception("appAttemptID is not specified for storm master");
        }

        KattaConfiguration conf = new KattaConfiguration("katta.node.properties");

        YarnConfiguration hadoopConf = new YarnConfiguration();

        KattaAMRMClient rmClient =
                new KattaAMRMClient(appAttemptID, conf, hadoopConf);


        KattaAppMaster server = new KattaAppMaster(conf, rmClient);
        try {
            final String host = server.getAvroServerHost();
            final int port = server.getAvroServerPort();
            InetAddress inetAddress = InetAddress.getByName(host);

            LOG.info("registration rm, app host name: " + inetAddress + ":" + port);

            RegisterApplicationMasterResponse resp =
                    rmClient.registerApplicationMaster(host, port, null);
            LOG.info("Got a registration response " + resp);
            LOG.info("Max Capability " + resp.getMaximumResourceCapability());

            server.initAndStartHeartbeat();

            LOG.info("Starting launcher");
            server.initAndStartLauncher();

            LOG.info("Starting Master Avro Server");
            server.serve();

            LOG.info("StormAMRMClient::unregisterApplicationMaster");
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
                    "AllDone", null);
        } catch (Exception e) {
            LOG.warn(ExceptionUtils.getFullStackTrace(e));
        } finally {
            server.stop();
            LOG.info("Stop Master Avro Server");

            rmClient.stop();
            LOG.info("Stop RM client");
        }
        System.exit(0);
    }

}
