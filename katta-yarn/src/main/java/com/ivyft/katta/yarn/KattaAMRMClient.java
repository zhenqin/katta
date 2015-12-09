

package com.ivyft.katta.yarn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ivyft.katta.protocol.metadata.Version;
import com.ivyft.katta.util.CollectionUtil;
import com.ivyft.katta.util.KattaConfiguration;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Records;

import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;

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
public class KattaAMRMClient extends AMRMClientImpl<ContainerRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(KattaAMRMClient.class);

    private final KattaConfiguration conf;

    private final YarnConfiguration hadoopConf;

    private final Priority DEFAULT_PRIORITY = Records.newRecord(Priority.class);

    //private final Set<Container> containers = new TreeSet<Container>();

    private final BlockingQueue<Container> CONTAINER_QUEUE = new LinkedBlockingQueue<Container>(3);


    private Container currentContainer;

    private final ReentrantLock LOCK = new ReentrantLock();

    private ApplicationAttemptId appAttemptId;

    private NMClient nmClient;

    public KattaAMRMClient(ApplicationAttemptId appAttemptId,
                           KattaConfiguration conf,
                           YarnConfiguration hadoopConf) {
        this.appAttemptId = appAttemptId;
        this.conf = conf;
        this.hadoopConf = hadoopConf;
        int pri = conf.getInt(KattaOnYarn.MASTER_CONTAINER_PRIORITY, 0);
        this.DEFAULT_PRIORITY.setPriority(pri);


        // start am nm client
        nmClient = NMClient.createNMClient();
        nmClient.init(hadoopConf);
        nmClient.start();


        this.init(hadoopConf);
        this.start();
    }



    public synchronized void setUp() {
        Resource resource = Resource.newInstance(512, 1);
        ContainerRequest req = new ContainerRequest(
                resource,
                null, // String[] nodes,
                null, // String[] racks,
                DEFAULT_PRIORITY);

        LOG.info("开始准备申请内存: " + req.toString());

        this.addContainerRequest(req);
    }



    public synchronized void addAllocatedContainers(List<Container> containers) {
        LOG.info("已申请到内存: " + containers.toString());

        /*for (int i = 0; i < containers.size(); i++) {
            Resource resource = Resource.newInstance(1024, 1);
            ContainerRequest req = new ContainerRequest(
                    resource,
                    null, // String[] nodes,
                    null, // String[] racks,
                    DEFAULT_PRIORITY);
            super.removeContainerRequest(req);
        }*/

        if(currentContainer != null) {
            containers.remove(currentContainer);
        }
        LOCK.lock();
        try {
            for (Container container : containers) {
                if(CONTAINER_QUEUE.contains(container)) {
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


    public void startMaster() {
        try {
            //申请 Container 内存
            this.setUp();

            currentContainer = CONTAINER_QUEUE.take();
            LOCK.lock();
            try {
                launchKattaMasterOnContainer(currentContainer);
            } finally {
                currentContainer = null;
                LOCK.unlock();
            }
        } catch (Exception e) {
            LOG.error("", e);
        }
    }





    public void startNode() {
        try {
            //申请 Container 内存
            this.setUp();

            currentContainer = CONTAINER_QUEUE.take();
            LOCK.lock();
            try {
                launchKattaNodeOnContainer(currentContainer);
            } finally {
                currentContainer = null;
                LOCK.unlock();
            }
        } catch (Exception e) {
            LOG.error("", e);
        }
    }





    private synchronized void releaseAllContainerRequest() {
        /*Iterator<Container> it = this.containers.iterator();
        ContainerId id;
        while (it.hasNext()) {
            id = it.next().getId();
            LOG.debug("Releasing container (id:" + id + ")");
            releaseAssignedContainer(id);
            it.remove();
        }*/
    }



    public void launchKattaMasterOnContainer(Container container)
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
        String katta_zip_path = conf.getProperty("katta.zip.path", "");

        Version kattaVersion = Version.readFromJar();
        LOG.info(kattaVersion.getRevision());

        FileSystem fs = FileSystem.get(this.hadoopConf);

        Path zip;
        if (StringUtils.isNotBlank(katta_zip_path)) {
            //自己指定的
            zip = new Path(katta_zip_path);
            if(!fs.exists(zip) || !fs.isFile(zip)) {
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
            LOG.info("Use NMClient to launch supervisors in container. ");
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



    public void launchKattaNodeOnContainer(Container container)
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
        String katta_zip_path = conf.getProperty("katta.zip.path", "");

        Version kattaVersion = Version.readFromJar();
        LOG.info(kattaVersion.getRevision());

        FileSystem fs = FileSystem.get(this.hadoopConf);

        Path zip;
        if (StringUtils.isNotBlank(katta_zip_path)) {
            //自己指定的
            zip = new Path(katta_zip_path);
            if(!fs.exists(zip) || !fs.isFile(zip)) {
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
        List<String> masterArgs = Util.buildNodeCommands(this.conf);

        LOG.info("node luanch: " + StringUtils.join(masterArgs, "  "));

        launchContext.setCommands(masterArgs);

        try {
            LOG.info("Use NMClient to launch supervisors in container. ");
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
