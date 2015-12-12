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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
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
public class KattaAppMaster implements Runnable, org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(KattaAppMaster.class);


    /**
     * 默认 Katta Master/Node 在 Yarn 任务优先级
     */
    private final Priority DEFAULT_PRIORITY = Records.newRecord(Priority.class);


    /**
     * AppMaster 当前是否调用 Stop() 方法, 防止重复调用
     */
    private boolean APP_MASTER_STOPPED = false;


    /**
     * Master Avro 命令服务器
     */
    private final KattaYarnAvroServer server;


    /**
     * Katta Conf
     */
    private final KattaConfiguration conf;


    /**
     * AppMaster Task ID
     */
    protected final ApplicationAttemptId appAttemptID;


    /**
     * ResourcesManager Client
     */
    private final AMRMClientAsync<AMRMClient.ContainerRequest> client;


    /**
     * Avro 协议
     */
    private final KattaYarnMasterProtocol protocol;


    /**
     * Katta Client
     */
    protected final KattaAMRMClient kattaAMRMClient;


    /**
     * 内部队列消息
     */
    private final BlockingQueue<Object> launcherQueue = new LinkedBlockingQueue<Object>();


    /**
     * 构造方法
     * @param conf Katta Conf
     * @param appAttemptID AppMaster Task ID
     */
    private KattaAppMaster(KattaConfiguration conf, ApplicationAttemptId appAttemptID) {
        this(conf, new YarnConfiguration(), appAttemptID);
    }


    /**
     * 构造方法
     * @param conf Katta Conf
     * @param hadoopConf Hadoop Conf
     * @param appAttemptID AppMaster Task ID
     */
    private KattaAppMaster(KattaConfiguration conf,
                           Configuration hadoopConf, ApplicationAttemptId appAttemptID) {
        this.conf = conf;
        this.appAttemptID = appAttemptID;

        int pri = conf.getInt(KattaOnYarn.MASTER_CONTAINER_PRIORITY, 0);
        this.DEFAULT_PRIORITY.setPriority(pri);

        int heartBeatIntervalMs = conf.getInt(KattaOnYarn.MASTER_HEARTBEAT_INTERVAL_MILLIS, 10000);


        this.kattaAMRMClient = new KattaAMRMClient(appAttemptID, conf, hadoopConf);

        this.client = AMRMClientAsync.createAMRMClientAsync(heartBeatIntervalMs, this);
        this.client.init(hadoopConf);
        this.client.start();


        this.protocol = new KattaYarnMasterProtocol(conf, kattaAMRMClient);
        this.server = new KattaYarnAvroServer(KattaYarnProtocol.class, protocol);
        int port = conf.getInt(KattaOnYarn.MASTER_AVRO_PORT, 4880);

        SocketPortFactory portFactory = new FreeSocketPortFactory();
        port = portFactory.getSocketPort(port, conf.getInt(KattaOnYarn.MASTER_AVRO_PORT + ".step", 1));
        this.server.setHost(NetworkUtils.getLocalhostName());
        this.server.setPort(port);

        LOG.info("katta application master start at: " + getAvroServerHost() + ":" + getAvroServerPort());

        //把使用的端口重新设置, 这样是防止在同一个 Node 上运行多个 AppMaster
        conf.setProperty(KattaOnYarn.MASTER_AVRO_PORT, port);

        protocol.setAppMaster(this);

        try {
            LOG.info("launch katta master");
            //protocol.startMaster(1);
        } catch (Exception e) {
            LOG.warn(ExceptionUtils.getFullStackTrace(e));
        }

        try {
            int numKattaNode =
                    conf.getInt(KattaOnYarn.DEFAULT_KATTA_NODE_NUM, 1);
            LOG.info("launch katta node, node num: " + numKattaNode);
            //protocol.startNode(numKattaNode);
        } catch (Exception e) {
            LOG.warn(ExceptionUtils.getFullStackTrace(e));
        }
    }


    public void serve() {
        this.server.serve();
    }


    public void stop() {
        if(!APP_MASTER_STOPPED) {
            LOG.info("Stop Katta App Master Avro Server");
            if (server != null) {
                try {
                    server.stop();
                } catch (Exception e) {
                    LOG.warn("", e);
                }
            }

            LOG.info("Stop Katta Masters");
            try {
                protocol.stopMaster();
            } catch (AvroRemoteException e) {
                LOG.warn("", e);
            }

            LOG.info("Stop Katta Nodes");
            try {
                protocol.stopAllNode();
            } catch (AvroRemoteException e) {
                LOG.warn("", e);
            }
        }

        APP_MASTER_STOPPED = true;
    }


    public ApplicationAttemptId getAppAttemptID() {
        return appAttemptID;
    }


    public void add(Object o) {
        launcherQueue.add(o);
    }

    @Override
    public void run() {
        while (!APP_MASTER_STOPPED && client.getServiceState() == Service.STATE.STARTED &&
                !Thread.currentThread().isInterrupted()) {
            try {
                Object take = launcherQueue.take();
                if(take instanceof Shutdown) {
                    LOG.warn("shutdown message, app master stopping.");
                    Thread.sleep(3000);
                    this.stop();
                    break;
                } else if(take instanceof Container) {
                    Container container = (Container) take;
                    LOG.info("LAUNCHER: Taking container with id (" + container.getId() + ") from the queue.");
                }
            } catch (InterruptedException e) {
                if (client.getServiceState() == Service.STATE.STARTED) {
                    LOG.error("Launcher thread interrupted : ", e);
                }
                break;
            } catch (Exception e) {
                LOG.error("Launcher thread I/O exception : ", e);
            }
        }

        LOG.warn("katta-app-master-worker-thread stoped.");
    }




    public synchronized void newContainer(int memory, int cores) {
        Resource resource = Resource.newInstance(memory, cores);
        AMRMClient.ContainerRequest req = new AMRMClient.ContainerRequest(
                resource,
                null, // String[] nodes,
                null, // String[] racks,
                DEFAULT_PRIORITY);

        LOG.info("开始准备申请内存: " + req.toString());

        this.client.addContainerRequest(req);
    }


    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        LOG.info("HB: Containers completed (" + statuses.size() + "), so releasing them.");
        LOG.info(statuses.toString());

        for (ContainerStatus statuse : statuses) {
            statuse.getContainerId();
        }
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        LOG.info("已申请到内存: " + containers.size());
        LOG.info(containers.toString());

        this.kattaAMRMClient.addAllocatedContainers(containers);
    }

    @Override
    public void onShutdownRequest() {
        LOG.info("onShutdownRequest(): katta app master shutdown");
        stop();
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
        LOG.info("updatedNodes(): " + updatedNodes);
    }

    @Override
    public float getProgress() {
        //ApplicationMaster 进度一直是 50%
        return 0.5f;
    }

    @Override
    public void onError(Throwable e) {
        LOG.warn("error print: ");
        LOG.error(ExceptionUtils.getFullStackTrace(e));
    }



    private void initAndStartLauncher() {
        Thread thread = new Thread(this);
        thread.setDaemon(true);
        thread.setName("katta-app-master-worker-thread");
        thread.start();
    }


    private void registerAppMaster() throws Exception {
        final String host = getAvroServerHost();
        final int port = getAvroServerPort();
        InetAddress inetAddress = InetAddress.getByName(host);

        LOG.info("registration rm, app host name: " + inetAddress + ":" + port);

        RegisterApplicationMasterResponse resp =
                client.registerApplicationMaster(host, port, null);
        LOG.info("Got a registration response " + resp);
        LOG.info("Max Capability " + resp.getMaximumResourceCapability());
    }





    private void finishAppMaster() throws Exception {
        try {
            client.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
                    "AllDone", null);
        } catch (Exception e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        }
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

        KattaAppMaster server = new KattaAppMaster(conf, hadoopConf, appAttemptID);
        try {
            server.registerAppMaster();

            LOG.info("Starting launcher");
            server.initAndStartLauncher();

            LOG.info("Starting Master Avro Server");
            server.serve();

            LOG.info("Katta AMRMClient::unregisterApplicationMaster");
            server.finishAppMaster();
        } catch (Exception e) {
            LOG.warn(ExceptionUtils.getFullStackTrace(e));
        } finally {
            LOG.info("Stop Katta Application Master.");
            server.stop();

        }
        System.exit(0);
    }

}
