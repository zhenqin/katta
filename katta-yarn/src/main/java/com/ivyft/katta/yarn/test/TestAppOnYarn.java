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

import com.ivyft.katta.protocol.metadata.Version;
import com.ivyft.katta.yarn.Util;
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


public class TestAppOnYarn {
    private static final Logger LOG = LoggerFactory.getLogger(TestAppOnYarn.class);
    final public static String YARN_REPORT_WAIT_MILLIS = "yarn.report.wait.millis";
    final public static String MASTER_HEARTBEAT_INTERVAL_MILLIS = "master.heartbeat.interval.millis";
    final public static String MASTER_HOST = "katta.master.host";
    final public static String MASTER_AVRO_PORT = "master.avro.port";
    final public static String DEFAULT_KATTA_NODE_NUM = "default.katta.node.num";
    final public static String MASTER_CONTAINER_PRIORITY = "master.container.priority";


    private YarnClient _yarn;
    private YarnConfiguration _hadoopConf;
    private ApplicationId _appId;
    private Map<String, String> conf;

    private TestAppOnYarn(Map<String, String> kattaConf) {
        this(null, kattaConf);
    }

    private TestAppOnYarn(ApplicationId appId, Map<String, String> kattaConf) {
        _hadoopConf = new YarnConfiguration();
        _yarn = YarnClient.createYarnClient();
        this.conf = kattaConf;
        _appId = appId;
        _yarn.init(_hadoopConf);
        _yarn.start();
    }

    public void stop() {
        _yarn.stop();
    }

    public ApplicationId getAppId() {
        return _appId;
    }


    private void launchApp(String appName, String queue, int amMB,
                           String katta_zip_location) throws Exception {
        LOG.debug("KattaOnYarn:launchApp() ...");
        YarnClientApplication client_app = _yarn.createApplication();
        GetNewApplicationResponse app = client_app.getNewApplicationResponse();
        _appId = app.getApplicationId();
        LOG.debug("_appId:"+_appId);

        if(amMB > app.getMaximumResourceCapability().getMemory()) {
            //TODO need some sanity checks
            amMB = app.getMaximumResourceCapability().getMemory();
        }
        ApplicationSubmissionContext appContext =
                Records.newRecord(ApplicationSubmissionContext.class);
        appContext.setApplicationId(app.getApplicationId());
        appContext.setApplicationName(appName);
        appContext.setQueue(queue);

        // Set up resource type requirements
        // For now, only memory is supported so we set memory requirements
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(amMB);
        capability.setVirtualCores(3);

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
        String appMasterJar = findContainingJar(TestAppOnYarn.class);
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
        conf.put("katta.zip.path", zip.makeQualified(fs).toUri().getPath());
        conf.put("katta.zip.visibility", "PUBLIC");
        if (!Util.isPublic(fs, zip)) {
            visibility = LocalResourceVisibility.APPLICATION;
            conf.put("katta.zip.visibility", "APPLICATION");
        }
        localResources.put("katta", Util.newYarnAppResource(fs, zip, LocalResourceType.ARCHIVE, visibility));


        Path confDst = Util.copyClasspathConf(fs, appHome);
        // establish a symbolic link to conf directory
        localResources.put("conf", Util.newYarnAppResource(fs, confDst));

        // Setup security tokens
        Path[] paths = new Path[3];
        paths[0] = dst;
        paths[1] = confDst;
        paths[2] = zip;

        Credentials credentials = new Credentials();
        TokenCache.obtainTokensForNamenodes(credentials, paths, _hadoopConf);
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

        //security tokens for HDFS distributed cache
        amContainer.setTokens(securityTokens);

        // Set local resource info into app master container launch context
        amContainer.setLocalResources(localResources);

        // Set the env variables to be setup in the env where the application master
        // will be run
        LOG.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<String, String>();
        // add the runtime classpath needed for tests to work
        //Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./conf");
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./AppMaster.jar");

        //Make sure that AppMaster has access to all YARN JARs
        List<String> yarn_classpath_cmd = Arrays.asList("yarn", "classpath");
        ProcessBuilder pb = new ProcessBuilder(yarn_classpath_cmd);
        LOG.info("YARN CLASSPATH COMMAND = [" + yarn_classpath_cmd + "]");
        pb.environment().putAll(System.getenv());
        Process proc = pb.start();
        Util.redirectStreamAsync(proc.getErrorStream(), System.err);
        BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream(), "UTF-8"));
        String line = "";
        String yarn_class_path = conf.get("katta.yarn.yarn_classpath");
        if (yarn_class_path == null){
            StringBuilder yarn_class_path_builder = new StringBuilder();
            while ((line = reader.readLine() ) != null){
                yarn_class_path_builder.append(line);
            }
            yarn_class_path = yarn_class_path_builder.toString();
        }
        LOG.info("YARN CLASSPATH = [" + yarn_class_path + "]");
        proc.waitFor();
        reader.close();
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(), yarn_class_path);



        Util.getKattaHomeInZip(fs, zip, kattaVersion.getNumber());
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./katta/" + "/*");
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./katta/" + "/lib/*");

        String java_home = conf.get("katta.yarn.java_home");
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
        vargs.add("java");


        vargs.add("-Dlogfile.name=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/master.log");
        //vargs.add("-verbose:class");
        vargs.add(TestAppMaster.class.getName());
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
        // Set java executable command
        LOG.info("Setting up app master command:" + vargs);

        amContainer.setCommands(vargs);

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

            // Check app status every 1 second.
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                LOG.debug("Thread sleep in monitoring loop interrupted");
            }

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

            //announce application master's host and port
            if (state==YarnApplicationState.RUNNING) {
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

        return new File("katta-yarn/target/katta-yarn.jar").toURI().toString();
                
        //throw new IOException("Fail to locat a JAR for class: "+my_class.getName());
    }

    public static TestAppOnYarn launchApplication(String appName,
                                                String queue,
                                                int amMB,
                                                Map<String, String> kattaConf,
                                                String katta_zip_location) throws Exception {
        TestAppOnYarn katta = new TestAppOnYarn(kattaConf);
        katta.launchApp(appName, queue, amMB, katta_zip_location);
        katta.waitUntilLaunched();
        return katta;
    }

    public static TestAppOnYarn attachToApp(String appId, Map<String, String> kattaConf) {
        return new TestAppOnYarn(ConverterUtils.toApplicationId(appId), kattaConf);
    }


    public static void main(String[] args) throws Exception {
        launchApplication("TestOnYarn",
                "default",
                3000,
                new HashMap<String, String>(),
                null);
    }
}
