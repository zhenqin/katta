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

import java.net.URL;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;

import java.io.OutputStreamWriter;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.base.Joiner;

class Util {
  private static final String KATTA_CONF_PATH_STRING = "conf" + Path.SEPARATOR + "katta.yaml";

  static String getKattaHome() {
      String ret = System.getProperty("katta.home");
      if (ret == null) {
        throw new RuntimeException("katta.home is not set");
      }
      return ret;
  }


  static String getKattaHomeInZip(FileSystem fs, Path zip, String kattaVersion) throws IOException, RuntimeException {
    FSDataInputStream fsInputStream = fs.open(zip);
    ZipInputStream zipInputStream = new ZipInputStream(fsInputStream);
    ZipEntry entry = zipInputStream.getNextEntry();
    while (entry != null) {
      String entryName = entry.getName();
      if (entryName.matches("^katta(-" + kattaVersion + ")?/")) {
        fsInputStream.close();
        return entryName.replace("/", "");
      }
      entry = zipInputStream.getNextEntry();
    }
    fsInputStream.close();
    throw new RuntimeException("Can not find katta home entry in katta zip file.");
  }

  static LocalResource newYarnAppResource(FileSystem fs, Path path,
      LocalResourceType type, LocalResourceVisibility vis) throws IOException {
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

  @SuppressWarnings("rawtypes")
  static void rmNulls(Map map) {
    Set s = map.entrySet();
    Iterator it = s.iterator();
    while (it.hasNext()) {
      Map.Entry m =(Map.Entry)it.next();
      if (m.getValue() == null) 
        it.remove();
    }
  }

  @SuppressWarnings("rawtypes")
  static Path createConfigurationFileInFs(FileSystem fs,
          String appHome, YarnConfiguration yarnConf)
          throws IOException {
    // dump stringwriter's content into FS conf/katta.yaml
    Path confDst = new Path(fs.getHomeDirectory(),
            appHome + Path.SEPARATOR + KATTA_CONF_PATH_STRING);
    Path dirDst = confDst.getParent();
    fs.mkdirs(dirDst);

    //katta.yaml
    FSDataOutputStream out;
    OutputStreamWriter writer;

    //yarn-site.xml
    Path yarn_site_xml = new Path(dirDst, "yarn-site.xml");
    out = fs.create(yarn_site_xml);
    writer = new OutputStreamWriter(out);
    yarnConf.writeXml(writer);
    writer.close();
    out.close();

    //logback.xml
    Path logback_xml = new Path(dirDst, "logback.xml");
    out = fs.create(logback_xml);
    CreateLogbackXML(out);
    out.close();

    return dirDst;
  } 

  static LocalResource newYarnAppResource(FileSystem fs, Path path)
      throws IOException {
    return Util.newYarnAppResource(fs, path, LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION);
  }

  private static void CreateLogbackXML(OutputStream out) throws IOException {
    Enumeration<URL> logback_xml_urls;
    logback_xml_urls = Thread.currentThread().getContextClassLoader().getResources("logback.xml");
    while (logback_xml_urls.hasMoreElements()) {
      URL logback_xml_url = logback_xml_urls.nextElement();
      if (logback_xml_url.getProtocol().equals("file")) {
        //Case 1: logback.xml as simple file
        FileInputStream is = new FileInputStream(logback_xml_url.getPath());
        while (is.available() > 0) {
          out.write(is.read());
        }
        is.close();
        return;
      }
      if (logback_xml_url.getProtocol().equals("jar")) {
        //Case 2: logback.xml included in a JAR
        String path = logback_xml_url.getPath();
        String jarFile = path.substring("file:".length(), path.indexOf("!"));
        java.util.jar.JarFile jar = new java.util.jar.JarFile(jarFile);
        Enumeration<JarEntry> enums = jar.entries();
        while (enums.hasMoreElements()) {
          JarEntry file = enums.nextElement();
          if (!file.isDirectory() && file.getName().equals("logback.xml")) {
            InputStream is = jar.getInputStream(file); // get the input stream
            while (is.available() > 0) {
              out.write(is.read());
            }
            is.close();
            jar.close();
            return;
          }
        }
        jar.close();
      }
    }

    throw new IOException("Failed to locate a logback.xml");
  }

  @SuppressWarnings("rawtypes")
  private static List<String> buildCommandPrefix(Map conf, String childOptsKey) 
          throws IOException {
      String kattaHomePath = getKattaHome();
      List<String> toRet = new ArrayList<String>();
      if (System.getenv("JAVA_HOME") != null)
        toRet.add(System.getenv("JAVA_HOME") + "/bin/java");
      else
        toRet.add("java");
      toRet.add("-server");
      toRet.add("-Dkatta.home=" + kattaHomePath);
      //toRet.add("-Djava.library.path=" + conf.get(backtype.katta.Config.JAVA_LIBRARY_PATH));
      toRet.add("-Dkatta.conf.file=" + new File(KATTA_CONF_PATH_STRING).getName());
      toRet.add("-cp");
      toRet.add(buildClassPathArgument());

      if (conf.containsKey(childOptsKey)
              && conf.get(childOptsKey) != null) {
          toRet.add((String) conf.get(childOptsKey));
      }

      return toRet;
  }


  @SuppressWarnings("rawtypes")
  static List<String> buildMasterCommands(Map conf) throws IOException {
      List<String> toRet =
              buildCommandPrefix(conf, "");

      toRet.add("-Dlogfile.name=" + System.getenv("KATTA_LOG_DIR") + "/nimbus.log");
      toRet.add("backtype.katta.daemon.nimbus");

      return toRet;
  }

  @SuppressWarnings("rawtypes")
  static List<String> buildSupervisorCommands(Map conf) throws IOException {
      List<String> toRet =
              buildCommandPrefix(conf, "");

      toRet.add("-Dworker.logdir="+ ApplicationConstants.LOG_DIR_EXPANSION_VAR);
      toRet.add("-Dlogfile.name=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/supervisor.log");
      toRet.add("backtype.katta.daemon.supervisor");

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

  static String getApplicationHomeForId(String id) {
      if (id.isEmpty()) {
          throw new IllegalArgumentException(
                  "The ID of the application cannot be empty.");
      }
      return ".katta" + Path.SEPARATOR + id;
  }

    /**
     * Returns a boolean to denote whether a cache file is visible to all(public)
     * or not
     * @param fs  Hadoop file system
     * @param path  file path
     * @return true if the path is visible to all, false otherwise
     * @throws IOException
     */
    static boolean isPublic(FileSystem fs, Path path) throws IOException {
        //the leaf level file should be readable by others
        if (!checkPermissionOfOther(fs, path, FsAction.READ)) {
            return false;
        }
        return ancestorsHaveExecutePermissions(fs, path.getParent());
    }

    /**
     * Checks for a given path whether the Other permissions on it
     * imply the permission in the passed FsAction
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
    static boolean ancestorsHaveExecutePermissions(FileSystem fs, Path path) throws IOException {
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
    
    static void redirectStreamAsync(final InputStream input, final PrintStream output) {
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
