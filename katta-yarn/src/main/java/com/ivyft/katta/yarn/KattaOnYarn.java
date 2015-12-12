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

import com.ivyft.katta.protocol.metadata.Version;
import com.ivyft.katta.util.KattaConfiguration;
import com.ivyft.katta.yarn.protocol.KattaYarnClient;
import org.apache.avro.AvroRemoteException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.util.*;

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
public class KattaOnYarn {
    private static final Logger LOG = LoggerFactory.getLogger(KattaOnYarn.class);
    public final static String YARN_REPORT_WAIT_MILLIS = "yarn.report.wait.millis";
    public final static String MASTER_HEARTBEAT_INTERVAL_MILLIS = "master.heartbeat.interval.millis";
    public final static String KATTA_MASTER_HOST = "yarn.katta.master.host";
    public final static String MASTER_AVRO_PORT = "yarn.appmaster.avro.port";
    public final static String DEFAULT_KATTA_NODE_NUM = "yarn.katta.node.default.num";
    public final static String MASTER_CONTAINER_PRIORITY = "yarn.master.container.priority";

    public final static String DEFAULE_SOLR_HOME = "./katta/data/solr";

    private YarnClient _yarn;
    private YarnConfiguration _hadoopConf;
    private ApplicationId _appId;
    private KattaConfiguration conf;
    private KattaYarnClient _client = null;

    private KattaOnYarn(KattaConfiguration kattaConf) {
        this(null, kattaConf);
    }

    private KattaOnYarn(ApplicationId appId, KattaConfiguration kattaConf) {
        _hadoopConf = new YarnConfiguration();  
        _yarn = YarnClient.createYarnClient();
        this.conf = kattaConf;
        _appId = appId;
        _yarn.init(_hadoopConf);
        _yarn.start();
    }

    public void stop() {
        if(_client != null) {
            try {
                _client.shutdown();
            } catch (AvroRemoteException e) {
                LOG.error(e.getMessage());
            }
        }
        _yarn.stop();
    }

    public ApplicationId getAppId() {
        return _appId;
    }

    public synchronized KattaYarnClient getClient() throws YarnException, IOException {
        if (_client == null) {
            String host = null;
            int port = 0;
            //wait for application to be ready
            int max_wait_for_report = conf.getInt(YARN_REPORT_WAIT_MILLIS, 60000);
            int waited = 0;
            while (waited < max_wait_for_report) {
                ApplicationReport report = _yarn.getApplicationReport(_appId);
                host = report.getHost();
                port = report.getRpcPort();
                LOG.info("application master start at " + host + ":" + port);
                if (host == null || port==0) { 
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                    waited += 1000;
                } else {
                    break;
                }
            }
            if (host == null || port==0) {
                LOG.info("No host/port returned for Application Master " + _appId);
                return null;
            }
            
            LOG.info("application report for "+_appId+" :"+host+":"+port);
            LOG.info("Attaching to "+host+":"+port+" to talk to app master "+_appId);
            _client = new KattaYarnClient(host, port);
        }
        return _client;
    }

    private void launchApp(String appName, String queue, int amMB,
                           String katta_zip_location) throws Exception {
        LOG.info("KattaOnYarn:launchApp() ...");
        YarnClientApplication client_app = _yarn.createApplication();
        GetNewApplicationResponse app = client_app.getNewApplicationResponse();
        _appId = app.getApplicationId();
        LOG.info("_appId:"+_appId);

        if(amMB > app.getMaximumResourceCapability().getMemory()) {
            //TODO need some sanity checks
            amMB = app.getMaximumResourceCapability().getMemory();
        }


        ApplicationSubmissionContext appContext = 
                Records.newRecord(ApplicationSubmissionContext.class);
        appContext.setApplicationId(app.getApplicationId());
        appContext.setApplicationName(appName);
        appContext.setQueue(queue);
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(amMB);
        capability.setVirtualCores(1);

        appContext.setResource(capability);

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
        String appMasterJar = findContainingJar(KattaAppMaster.class);
        LOG.info("appMasterJar: " + appMasterJar);

        FileSystem fs = FileSystem.get(_hadoopConf);
        Path src = new Path(appMasterJar);
        String appHome =  Util.getApplicationHomeForId(_appId.toString());
        Path dst = new Path(fs.getHomeDirectory(), 
                appHome + Path.SEPARATOR + "AppMaster.jar");
        fs.copyFromLocalFile(false, true, src, dst);
        LOG.info("copy jar from: " + src + " to: " + dst);
        localResources.put("AppMaster.jar", Util.newYarnAppResource(fs, dst));

        Version kattaVersion = Version.readFromJar();
        LOG.info(kattaVersion.getRevision());

        Path zip;
        if (StringUtils.isNotBlank(katta_zip_location)) {
            //自己指定的
            zip = new Path(katta_zip_location);
            if(!fs.exists(zip) || !fs.isFile(zip)) {
                throw new IllegalArgumentException("katta location not exists. " + katta_zip_location);
            }

        } else {
            zip = new Path("/lib/katta/katta-" + kattaVersion.getRevision() + ".zip");
        }

        LocalResourceVisibility visibility = LocalResourceVisibility.PUBLIC;
        conf.setProperty("katta.zip.path", zip.makeQualified(fs).toUri().getPath());
        conf.setProperty("katta.zip.visibility", "PUBLIC");
        if (!Util.isPublic(fs, zip)) {
            visibility = LocalResourceVisibility.APPLICATION;
            conf.setProperty("katta.zip.visibility", "APPLICATION");
        }
        localResources.put("katta", Util.newYarnAppResource(fs, zip, LocalResourceType.ARCHIVE, visibility));

        Path confDst = Util.copyClasspathConf(fs, appHome);
        // establish a symbolic link to conf directory
        localResources.put("conf", Util.newYarnAppResource(fs, confDst));

        int rs = 3;
        // Setup security tokens
        Path[] paths = new Path[rs];
        paths[0] = dst;
        paths[1] = zip;
        paths[2] = confDst;

        Credentials credentials = new Credentials();
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

        TokenCache.obtainTokensForNamenodes(credentials, paths, _hadoopConf);
        //security tokens for HDFS distributed cache
        amContainer.setTokens(securityTokens);

        // Set local resource info into app master container launch context
        amContainer.setLocalResources(localResources);

        // Set the env variables to be setup in the env where the application master
        // will be run
        LOG.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<String, String>();
        // add the runtime classpath needed for tests to work
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./conf");
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./AppMaster.jar");

        Util.getKattaHomeInZip(fs, zip, kattaVersion.getNumber());
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./katta/" + "/*");
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./katta/" + "/lib/*");

        String java_home = conf.getProperty("katta.yarn.java_home", "");
        if (StringUtils.isNotBlank(java_home)) {
            env.put("JAVA_HOME", java_home);
        }

        LOG.info("Using JAVA_HOME = [" + env.get("JAVA_HOME") + "]");
        
        env.put("appJar", appMasterJar);
        env.put("appName", appName);
        env.put("appId", new Integer(_appId.getId()).toString());
        env.put("KATTA_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);

        amContainer.setEnvironment(env);

        // Set the necessary command to execute the application master
        Vector<String> vargs = new Vector<String>();
        vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
        //vargs.add("-Dkatta.node.hostname.overwritten=$hostname");
        vargs.add("-Dkatta.root.logger=INFO,DRFA");
        vargs.add("-Dkatta.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        vargs.add("-Dkatta.log.file=katta-on-yarn.log");
        //vargs.add("-verbose:class");
        vargs.add(com.ivyft.katta.yarn.KattaAppMaster.class.getName());
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
        // Set java executable command
        LOG.info("Setting up app master command:" + vargs);

        amContainer.setCommands(vargs);

        // Set up resource type requirements
        // For now, only memory is supported so we set memory requirements

        appContext.setAMContainerSpec(amContainer);
        //appContext.setUnmanagedAM(true);

        _yarn.submitApplication(appContext);
    }


    /**
     * Wait until the application is successfully launched
     * @throws YarnException
     */
    public boolean waitUntilLaunched() throws YarnException, IOException {
        while (true) {
            // Get application report for the appId we are interested in
            ApplicationReport report = _yarn.getApplicationReport(_appId);

            LOG.info(report.toString());


            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    LOG.info("Application has completed successfully. Breaking monitoring loop");
                    return true;        
                }
                else {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop");
                    return false;
                }             
            } else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish."
                        + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop");
                return false;
            }

            // Check app status every 1 second.
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                LOG.debug("Thread sleep in monitoring loop interrupted");
            }

            //announce application master's host and port
            if (state == YarnApplicationState.RUNNING) {
                LOG.info(report.getApplicationId() + " luanched, status: " + state);
                return true;
            }
        }
    }


    /** 
     * Find a jar that contains a class of the same name, if any.
     * It will return a jar file, even if that is not the first thing
     * on the class path that has a class with the same name.
     * 
     * @param my_class the class to find.
     * @return a jar file that contains the class, or null.
     * @throws IOException on any error
     */
    public static String findContainingJar(Class<?> my_class) throws IOException {
        String appJar = System.getProperty("app.jar");
        if(StringUtils.isNotBlank(appJar)) {
            //katta-yarn/target/katta-yarn.jar
            return new File(appJar).toURI().toString();
        }
        ClassLoader loader = my_class.getClassLoader();
        String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
        for(Enumeration<URL> itr = loader.getResources(class_file);
                itr.hasMoreElements();) {
            URL url = itr.nextElement();
            if ("jar".equals(url.getProtocol())) {
                String toReturn = url.getPath();
                if (toReturn.startsWith("file:")) {
                    toReturn = toReturn.substring("file:".length());
                }
                // URLDecoder is a misnamed class, since it actually decodes
                // x-www-form-urlencoded MIME type rather than actual
                // URL encoding (which the file path has). Therefore it would
                // decode +s to ' 's which is incorrect (spaces are actually
                // either unencoded or encoded as "%20"). Replace +s first, so
                // that they are kept sacred during the decoding process.
                toReturn = toReturn.replaceAll("\\+", "%2B");
                toReturn = URLDecoder.decode(toReturn, "UTF-8");
                return toReturn.replaceAll("!.*$", "");
            }
        }
        throw new IOException("Fail to locat a JAR for class: "+my_class.getName());
    }

    public static KattaOnYarn launchApplication(String appName,
                                                String queue,
                                                int amMB,
                                                KattaConfiguration kattaConf,
                                                String katta_zip_location) throws Exception {
        KattaOnYarn katta = new KattaOnYarn(kattaConf);
        katta.launchApp(appName, queue, amMB, katta_zip_location);
        katta.waitUntilLaunched();
        return katta;
    }

    public static KattaOnYarn attachToApp(String appId, KattaConfiguration kattaConf) {
        return new KattaOnYarn(ConverterUtils.toApplicationId(appId), kattaConf);
    }


    public static void main(String[] args) throws Exception {
        KattaOnYarn.launchApplication("KattaOnYarn",
                "default",
                512,
                new KattaConfiguration("katta.node.properties"),
                null);
    }
}
