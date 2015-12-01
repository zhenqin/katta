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

import com.google.common.base.Joiner;
import com.ivyft.katta.Katta;
import com.ivyft.katta.util.KattaConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.*;
import java.net.InterfaceAddress;
import java.net.URL;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


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
public class Util {
    private static final String KATTA_CONF_PATH_STRING = "conf" + Path.SEPARATOR + "katta.yaml";

    public static String getKattaHome() {
        String ret = System.getProperty("katta.home");
        if (ret == null) {
            throw new RuntimeException("katta.home is not set");
        }
        return ret;
    }



    public static String getKattaHomeInZip(FileSystem fs, Path zip, String kattaVersion) throws IOException, RuntimeException {
        FSDataInputStream fsInputStream = fs.open(zip);
        ZipInputStream zipInputStream = new ZipInputStream(fsInputStream);
        ZipEntry entry = zipInputStream.getNextEntry();
        Set<String> jar = new HashSet<String>();
        jar.add("katta-client");
        jar.add("katta-core");

        while (entry != null) {
            String entryName = entry.getName();
            if(entryName.startsWith("katta")) {
                Iterator<String> iterator = jar.iterator();
                while (iterator.hasNext()) {
                    if(entryName.startsWith(iterator.next())) {
                        iterator.remove();
                        break;
                    }
                }
            }
            entry = zipInputStream.getNextEntry();
        }
        fsInputStream.close();

        if(!jar.isEmpty()) {
            throw new RuntimeException("Can not find katta home entry in katta-core, katta-client zip file.");
        }
        return "";
    }


    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        System.out.println(getKattaHomeInZip(fs, new Path("/lib/katta/katta-1.1.0.zip"), "1.1.0"));
    }


    public static List<String> getKattaHomeJars(FileSystem fs, Path kattaHome, String kattaVersion) throws IOException, RuntimeException {
        List<String> jars = new ArrayList<String>();
        PathFilter jarFilter = new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().endsWith(".jar");
            }
        };
        FileStatus[] statuses = fs.listStatus(kattaHome, jarFilter);

        if(statuses != null) {
            for (FileStatus statuse : statuses) {
                String path = statuse.getPath().toUri().getPath();
                jars.add(path.replaceAll(kattaHome.toString(), ""));
            }
        }

        statuses = fs.listStatus(new Path(kattaHome, "lib"), jarFilter);

        if(statuses != null) {
            for (FileStatus statuse : statuses) {
                String path = statuse.getPath().toUri().getPath();
                jars.add(path.replaceAll(kattaHome.toString(), ""));
            }
        }
        return jars;
    }


    public static LocalResource newYarnAppResource(FileSystem fs, Path path)
            throws IOException {
        return Util.newYarnAppResource(fs, path, LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION);
    }


    public static LocalResource newYarnAppResource(FileSystem fs,
                                            Path path,
                                            LocalResourceType type,
                                            LocalResourceVisibility vis) throws IOException {
        Path qualified = fs.makeQualified(path);
        FileStatus status = fs.getFileStatus(qualified);
        LocalResource resource = Records.newRecord(LocalResource.class);
        resource.setType(type);
        resource.setVisibility(vis);
        resource.setResource(ConverterUtils.getYarnUrlFromPath(qualified));
        resource.setTimestamp(status.getModificationTime());
        resource.setSize(status.getLen());
        return resource;
    }


    public static Path copyClasspathConf(FileSystem fs,
                                            String appHome)
            throws IOException {
        Path confDst = new Path(fs.getHomeDirectory(),
                appHome + Path.SEPARATOR + KATTA_CONF_PATH_STRING);
        Path dirDst = confDst.getParent();
        fs.mkdirs(dirDst);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if(classLoader == null) {
            classLoader = Util.class.getClassLoader();
        }

        URL classpath = classLoader.getResource("");

        File file = new File(classpath.getPath());
        File[] files = file.listFiles();
        if(files != null) {
            for (File file1 : files) {
                if(file1.isDirectory()) {
                    continue;
                }
                fs.copyFromLocalFile(new Path(file1.getAbsolutePath()), new Path(dirDst, file1.getName()));
            }
        }

        return dirDst;
    }


    private static List<String> buildCommandPrefix(KattaConfiguration conf)
            throws IOException {
        //String kattaHomePath = getKattaHome();
        List<String> toRet = new ArrayList<String>();
        String java_home = conf.getProperty("katta.yarn.java_home", "");
        if (StringUtils.isNotBlank(java_home)) {
            toRet.add("$JAVA_HOME=" + java_home);
        } else {
            toRet.add("java");
        }
        toRet.add("-server");
        //toRet.add("-Dkatta.home=" + getKattaHome());
        //toRet.add("-cp");
        //toRet.add(buildClassPathArgument());

        return toRet;
    }


    public static List<String> buildMasterCommands(KattaConfiguration conf) throws IOException {
        List<String> toRet = buildCommandPrefix(conf);
        toRet.add("-Dkatta.root.logger=INFO,DRFA");

        toRet.add("-Dkatta.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        toRet.add("-Dkatta.log.file=" + "katta-master-" + java.net.InetAddress.getLocalHost().getHostName() + ".log");
        toRet.add(Katta.class.getName());
        toRet.add("master");
        return toRet;
    }

    public static List<String> buildSupervisorCommands(KattaConfiguration conf) throws IOException {
        List<String> toRet = buildCommandPrefix(conf);

        toRet.add("-Dkatta.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        toRet.add("-Dkatta.log.file=" + "katta-node-" + java.net.InetAddress.getLocalHost().getHostName() + ".log");
        toRet.add(Katta.class.getName());
        toRet.add("node");
        return toRet;
    }

    private static String buildClassPathArgument() throws IOException {
        List<String> paths = new ArrayList<String>();
        paths.add(new File(KATTA_CONF_PATH_STRING).getParent());
        paths.add(getKattaHome());
        for (String jarPath : findAllJarsInPaths(getKattaHome(), getKattaHome() + File.separator + "lib")) {
            paths.add(jarPath);
        }
        return Joiner.on(File.pathSeparatorChar).join(paths);
    }

    private static interface FileVisitor {
        public void visit(File file);
    }

    private static List<String> findAllJarsInPaths(String... pathStrs) {
        final LinkedHashSet<String> pathSet = new LinkedHashSet<String>();

        FileVisitor visitor = new FileVisitor() {

            @Override
            public void visit(File file) {
                String name = file.getName();
                if (name.endsWith(".jar")) {
                    pathSet.add(file.getPath());
                }
            }
        };

        for (String path : pathStrs) {
            File file = new File(path);
            traverse(file, visitor);
        }

        final List<String> toRet = new ArrayList<String>();
        for (String p : pathSet) {
            toRet.add(p);
        }
        return toRet;
    }

    private static void traverse(File file, FileVisitor visitor) {
        if (file.isDirectory()) {
            File childs[] = file.listFiles();
            if (childs.length > 0) {
                for (int i = 0; i < childs.length; i++) {
                    File child = childs[i];
                    traverse(child, visitor);
                }
            }
        } else {
            visitor.visit(file);
        }
    }

    public static String getApplicationHomeForId(String id) {
        if (id.isEmpty()) {
            throw new IllegalArgumentException(
                    "The ID of the application cannot be empty.");
        }
        return ".katta" + Path.SEPARATOR + id;
    }

    /**
     * Returns a boolean to denote whether a cache file is visible to all(public)
     * or not
     *
     * @param fs   Hadoop file system
     * @param path file path
     * @return true if the path is visible to all, false otherwise
     * @throws IOException
     */
    public static boolean isPublic(FileSystem fs, Path path) throws IOException {
        //the leaf level file should be readable by others
        if (!checkPermissionOfOther(fs, path, FsAction.READ)) {
            return false;
        }
        return ancestorsHaveExecutePermissions(fs, path.getParent());
    }

    /**
     * Checks for a given path whether the Other permissions on it
     * imply the permission in the passed FsAction
     *
     * @param fs
     * @param path
     * @param action
     * @return true if the path in the uri is visible to all, false otherwise
     * @throws IOException
     */
    private static boolean checkPermissionOfOther(FileSystem fs, Path path,
                                                  FsAction action) throws IOException {
        FileStatus status = fs.getFileStatus(path);
        FsPermission perms = status.getPermission();
        FsAction otherAction = perms.getOtherAction();
        if (otherAction.implies(action)) {
            return true;
        }
        return false;
    }

    /**
     * Returns true if all ancestors of the specified path have the 'execute'
     * permission set for all users (i.e. that other users can traverse
     * the directory hierarchy to the given path)
     */
    public static boolean ancestorsHaveExecutePermissions(FileSystem fs, Path path) throws IOException {
        Path current = path;
        while (current != null) {
            //the subdirs in the path should have execute permissions for others
            if (!checkPermissionOfOther(fs, current, FsAction.EXECUTE)) {
                return false;
            }
            current = current.getParent();
        }
        return true;
    }

    public static void redirectStreamAsync(final InputStream input, final PrintStream output) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                Scanner scanner = new Scanner(input);
                while (scanner.hasNextLine()) {
                    output.println(scanner.nextLine());
                }
            }
        }).start();
    }
}
