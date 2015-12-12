

package com.ivyft.katta.yarn;

import com.ivyft.katta.protocol.metadata.Version;
import com.ivyft.katta.util.KattaConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;


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
public class KattaAMRMClient implements org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(KattaAMRMClient.class);

    private final KattaConfiguration conf;

    private final Configuration hadoopConf;

    //private final Set<Container> containers = new TreeSet<Container>();

    private final BlockingQueue<Container> CONTAINER_QUEUE = new LinkedBlockingQueue<Container>(3);


    private Container currentContainer;

    private final ReentrantLock LOCK = new ReentrantLock();

    private ApplicationAttemptId appAttemptId;

    private NMClient nmClient;

    public KattaAMRMClient(ApplicationAttemptId appAttemptId,
                           KattaConfiguration conf,
                           Configuration hadoopConf) {
        this.appAttemptId = appAttemptId;
        this.conf = conf;
        this.hadoopConf = hadoopConf;

        // start am nm client
        nmClient = NMClient.createNMClient();
        nmClient.init(hadoopConf);
        nmClient.start();

    }

    public synchronized void addAllocatedContainers(List<Container> containers) {
        if (currentContainer != null) {
            containers.remove(currentContainer);
        }
        LOCK.lock();
        try {
            for (Container container : containers) {
                if (CONTAINER_QUEUE.contains(container)) {
                    continue;
                }

                this.CONTAINER_QUEUE.put(container);
            }
        } catch (InterruptedException e) {
            LOG.info("", e);
        } finally {
            LOCK.unlock();
        }
    }


    public void startMaster(String kattaZip) {
        try {
            currentContainer = CONTAINER_QUEUE.take();
            LOCK.lock();
            try {
                launchKattaMasterOnContainer(currentContainer, kattaZip);
            } finally {
                currentContainer = null;
                LOCK.unlock();
            }
        } catch (Exception e) {
            LOG.error("", e);
        }
    }


    /**
     * 启动 Yarn Katta Node
     *
     * @param kattaZip Katta-Home.zip
     * @param solrZip  Solr/Home zip
     */
    public void startNode(String kattaZip, String solrZip) {
        try {
            currentContainer = CONTAINER_QUEUE.take();
            LOCK.lock();
            try {
                launchKattaNodeOnContainer(currentContainer, kattaZip, solrZip);
            } finally {
                currentContainer = null;
                LOCK.unlock();
            }
        } catch (Exception e) {
            LOG.error("", e);
        }
    }


    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
        LOG.info("onContainerStarted: " + containerId.toString());
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
        LOG.info("onContainerStatusReceived: " + containerId.toString() + "    " + containerStatus);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
        LOG.info("onContainerStopped: " + containerId.toString());
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
        LOG.info("onStartContainerError: " + containerId.toString());
        LOG.warn(ExceptionUtils.getFullStackTrace(t));
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
        LOG.info("onGetContainerStatusError: " + containerId.toString());
        LOG.warn(ExceptionUtils.getFullStackTrace(t));
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
        LOG.info("onStopContainerError: " + containerId.toString());
        LOG.warn(ExceptionUtils.getFullStackTrace(t));
    }


    public void launchKattaMasterOnContainer(Container container, String katta_zip_path)
            throws IOException {
        //Path[] paths = null;
        // create a container launch context
        ContainerLaunchContext launchContext = Records.newRecord(ContainerLaunchContext.class);
        UserGroupInformation user = UserGroupInformation.getCurrentUser();
        try {
            Credentials credentials = user.getCredentials();
            //TokenCache.obtainTokensForNamenodes(credentials, paths, hadoopConf);

            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            launchContext.setTokens(securityTokens);
        } catch (IOException e) {
            LOG.warn("Getting current user info failed when trying to launch the container"
                    + e.getMessage());
        }

        // CLC: local resources includes katta, conf
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        //String katta_zip_path = conf.getProperty("katta.zip.path", "");

        Version kattaVersion = Version.readFromJar();
        LOG.info(kattaVersion.getRevision());

        FileSystem fs = FileSystem.get(this.hadoopConf);

        Path zip;
        if (StringUtils.isNotBlank(katta_zip_path)) {
            //自己指定的
            zip = new Path(katta_zip_path);
            if (!fs.exists(zip) || !fs.isFile(zip)) {
                throw new IllegalArgumentException("katta location not exists. " + katta_zip_path);
            }

        } else {
            zip = new Path("/lib/katta/katta-" + kattaVersion.getRevision() + ".zip");
        }

        LOG.info("katta.home=" + zip.toString());


        String vis = conf.getProperty("katta.zip.visibility", "PUBLIC");
        if (vis.equals("PUBLIC"))
            localResources.put("katta", Util.newYarnAppResource(fs, zip,
                    LocalResourceType.ARCHIVE, LocalResourceVisibility.PUBLIC));
        else if (vis.equals("PRIVATE"))
            localResources.put("katta", Util.newYarnAppResource(fs, zip,
                    LocalResourceType.ARCHIVE, LocalResourceVisibility.PRIVATE));
        else if (vis.equals("APPLICATION"))
            localResources.put("katta", Util.newYarnAppResource(fs, zip,
                    LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION));

        String appHome = Util.getApplicationHomeForId(appAttemptId.toString());
        String containerHome = appHome + Path.SEPARATOR + container.getId().getId();

        Path confDst = Util.copyClasspathConf(fs, containerHome);
        localResources.put("conf", Util.newYarnAppResource(fs, confDst));


        // CLC: env
        Map<String, String> env = new HashMap<String, String>();
        env.put("KATTA_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        //env.put("appId", new Integer(_appId.getId()).toString());

        Util.getKattaHomeInZip(fs, zip, kattaVersion.getNumber());
        Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), "./conf");
        Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), "./katta/*");
        Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), "./katta/lib/*");


        launchContext.setEnvironment(env);
        launchContext.setLocalResources(localResources);

        // CLC: command
        List<String> masterArgs = Util.buildMasterCommands(this.conf);

        LOG.info("master luanch: " + StringUtils.join(masterArgs, "  "));

        launchContext.setCommands(masterArgs);

        try {
            LOG.info("Use NMClient to launch katta master in container. ");
            Map<String, ByteBuffer> result = nmClient.startContainer(container, launchContext);

            LOG.info("luanch result: " + result);

            String userShortName = user.getShortUserName();
            if (userShortName != null)
                LOG.info("Master log: http://" + container.getNodeHttpAddress() + "/node/containerlogs/"
                        + container.getId().toString() + "/" + userShortName + "/master.log");
        } catch (Exception e) {
            LOG.error("Caught an exception while trying to start a container", e);
            throw new IllegalArgumentException(e);
        }
    }


    public void launchKattaNodeOnContainer(Container container, String katta_zip_path, String solrZip)
            throws IOException {
        //Path[] paths = null;
        // create a container launch context
        ContainerLaunchContext launchContext = Records.newRecord(ContainerLaunchContext.class);
        UserGroupInformation user = UserGroupInformation.getCurrentUser();
        try {
            Credentials credentials = user.getCredentials();
            //TokenCache.obtainTokensForNamenodes(credentials, paths, hadoopConf);

            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            launchContext.setTokens(securityTokens);
        } catch (IOException e) {
            LOG.warn("Getting current user info failed when trying to launch the container"
                    + e.getMessage());
        }

        // CLC: local resources includes katta, conf
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        //String katta_zip_path = conf.getProperty("katta.zip.path", "");

        Version kattaVersion = Version.readFromJar();
        LOG.info(kattaVersion.getRevision());

        FileSystem fs = FileSystem.get(this.hadoopConf);

        Path zip;
        if (StringUtils.isNotBlank(katta_zip_path)) {
            //自己指定的
            zip = new Path(katta_zip_path);
            if (!fs.exists(zip) || !fs.isFile(zip)) {
                throw new IllegalArgumentException("katta location not exists. " + katta_zip_path);
            }

        } else {
            zip = new Path("/lib/katta/katta-" + kattaVersion.getRevision() + ".zip");
        }

        LOG.info("katta.home=" + zip.toString());


        String vis = conf.getProperty("katta.zip.visibility", "PUBLIC");
        if (vis.equals("PUBLIC"))
            localResources.put("katta", Util.newYarnAppResource(fs, zip,
                    LocalResourceType.ARCHIVE, LocalResourceVisibility.PUBLIC));
        else if (vis.equals("PRIVATE"))
            localResources.put("katta", Util.newYarnAppResource(fs, zip,
                    LocalResourceType.ARCHIVE, LocalResourceVisibility.PRIVATE));
        else if (vis.equals("APPLICATION"))
            localResources.put("katta", Util.newYarnAppResource(fs, zip,
                    LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION));

        String appHome = Util.getApplicationHomeForId(appAttemptId.toString());
        String containerHome = appHome + Path.SEPARATOR + container.getId().getId();

        Path confDst = Util.copyClasspathConf(fs, containerHome);
        localResources.put("conf", Util.newYarnAppResource(fs, confDst));


        // CLC: env
        Map<String, String> env = new HashMap<String, String>();
        env.put("KATTA_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        //env.put("appId", new Integer(_appId.getId()).toString());

        Util.getKattaHomeInZip(fs, zip, kattaVersion.getNumber());
        Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), "./conf");
        Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), "./katta/*");
        Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), "./katta/lib/*");


        launchContext.setEnvironment(env);
        launchContext.setLocalResources(localResources);

        // CLC: command
        if (StringUtils.isBlank(solrZip)) {
            solrZip = conf.getProperty("solr.solr.home", solrZip);
        }

        if (StringUtils.isBlank(solrZip)) {
            throw new IllegalStateException("can not find solr home." + solrZip);
        }

        List<String> masterArgs = Util.buildNodeCommands(this.conf, solrZip);

        LOG.info("node luanch: " + StringUtils.join(masterArgs, "  "));

        launchContext.setCommands(masterArgs);

        try {
            LOG.info("Use NMClient to launch katta node in container. ");
            Map<String, ByteBuffer> result = nmClient.startContainer(container, launchContext);

            LOG.info("luanch result: " + result);

            String userShortName = user.getShortUserName();
            if (userShortName != null)
                LOG.info("node log: http://" + container.getNodeHttpAddress() + "/node/containerlogs/"
                        + container.getId().toString() + "/" + userShortName + "/node.log");
        } catch (Exception e) {
            LOG.error("Caught an exception while trying to start a container", e);
            throw new IllegalArgumentException(e);
        }
    }
}
