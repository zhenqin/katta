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

package com.ivyft.katta.yarn.test;

import com.ivyft.katta.yarn.KattaAppMaster;
import com.ivyft.katta.yarn.KattaOnYarn;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.ContainerManagementProtocol;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TestAppMaster implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(TestAppMaster.class);


    private final AMRMClientImpl<AMRMClient.ContainerRequest> client;


    private Map<String, String> conf;


    Thread thread = new Thread(this);

    public TestAppMaster(Map<String, String> conf, AMRMClientImpl<AMRMClient.ContainerRequest> client) {
        this.client = client;
        this.conf = conf;
    }


    public void serve() {
        try {
            thread.join();
        } catch (InterruptedException e) {
            LOG.warn("", e);
        }
    }

    @Override
    public void run() {
        try {
            int heartBeatIntervalMs = 10000;

            while (client.getServiceState() == Service.STATE.STARTED &&
                    !Thread.currentThread().isInterrupted()) {

                Thread.sleep(heartBeatIntervalMs);

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


                LOG.info(allocatedContainers.toString());
                LOG.info("HB: Received allocated containers (" + allocatedContainers.size() + ")");

                if (allocatedContainers.size() > 0) {
                    //有资源? 等于0说明没资源了
                    // Add newly allocated containers to the client.
                    LOG.info("HB: Received allocated containers (" + allocatedContainers.size() + ")");
                }

                List<ContainerStatus> completedContainers =
                        allocResponse.getCompletedContainersStatuses();

                LOG.info("HB: Containers completed (" + completedContainers.size() + "), so releasing them.");
            }
        } catch (Throwable t) {
            // Something happened we could not handle.  Make sure the AM goes
            // down so that we are not surprised later on that our heart
            // stopped..
            LOG.error("Unhandled error in AM: ", t);
            System.exit(1);
        }
    }

    private void initAndStartLauncher() {
        NMClient nmClient = NMClient.createNMClient();
        AMRMClient<AMRMClient.ContainerRequest> amrmClient = AMRMClient.createAMRMClient();


        YarnConfiguration hadoopConf = new YarnConfiguration();
        nmClient.init(hadoopConf);
        nmClient.start();
        amrmClient.init(hadoopConf);
        amrmClient.start();

        //amrmClient.unregisterApplicationMaster();

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = Records
                .newRecord(ContainerLaunchContext.class);


        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of the
        // local resources
        LOG.info("Copy App Master jar from local filesystem and add to local environment");
        // Copy the application master jar to the filesystem
        // Create a local resource to point to the destination jar path
        String appMasterJar = null;
        try {
            appMasterJar = KattaOnYarn.findContainingJar(KattaAppMaster.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info("appMasterJar: " + appMasterJar);

        //nmClient.startContainer(, amContainer);
    }


    private Thread initAndStartHeartbeat() {
        thread.setDaemon(true);
        thread.setName("katta-app-master-heartbeat");
        thread.start();
        return thread;
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

        Map<String, String> conf = new HashMap<String, String>();//Config.readStormConfig(null);
        //Util.rmNulls(storm_conf);

        YarnConfiguration hadoopConf = new YarnConfiguration();

        final String host = InetAddress.getLocalHost().getHostName();
        //storm_conf.put("nimbus.host", host);


        AMRMClientImpl<AMRMClient.ContainerRequest> rmClient = new AMRMClientImpl<AMRMClient.ContainerRequest>();
        rmClient.init(hadoopConf);
        rmClient.start();

        TestAppMaster server = new TestAppMaster(conf, rmClient);
        try {
            final int port = 9090;
            final String target = host + ":" + port;
            InetSocketAddress addr = NetUtils.createSocketAddr(target);


            RegisterApplicationMasterResponse resp =
                    rmClient.registerApplicationMaster(addr.getHostName(), port, null);
            LOG.info("Got a registration response " + resp);
            LOG.info("Max Capability " + resp.getMaximumResourceCapability());

            server.initAndStartHeartbeat();

            server.serve();
        } catch (Exception e){
            LOG.warn(ExceptionUtils.getFullStackTrace(e));
        } finally {
            LOG.info("StormAMRMClient::unregisterApplicationMaster");
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
                    "AllDone", null);

            LOG.info("Stop RM client");
            rmClient.stop();
        }
        System.exit(0);
    }

}
