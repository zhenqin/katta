/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ivyft.katta.protocol.metadata;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Version of a cluster or distribution (depending if loaded from jar or from
 * the cluster itself).
 *
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-13
 * Time: 上午8:58
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class Version implements Serializable {


    /**
     * 序列化版本
     */
    private static final long serialVersionUID = 1L;


    /**
     * The version of Katta.
     */
    private String number;


    /**
     * 当前版本
     */
    private String revision;


    /**
     * 编译日期
     */
    private String compiledBy;


    /**
     * 编译人
     */
    private String compileTime;


    /**
     * log
     */
    private static final Logger LOG = LoggerFactory.getLogger(Version.class);


    /**
     * 默认构造方法
     */
    public Version() {
    }


    /**
     * 全构造方法
     * @param number
     * @param revision
     * @param compiledBy
     * @param compileTime
     */
    public Version(String number,
                   String revision,
                   String compiledBy,
                   String compileTime) {
        this.number = number;
        this.revision = revision;
        this.compiledBy = compiledBy;
        this.compileTime = compileTime;
    }


    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getRevision() {
        return revision;
    }

    public void setRevision(String revision) {
        this.revision = revision;
    }

    public String getCompiledBy() {
        return compiledBy;
    }

    public void setCompiledBy(String compiledBy) {
        this.compiledBy = compiledBy;
    }

    public String getCompileTime() {
        return compileTime;
    }

    public void setCompileTime(String compileTime) {
        this.compileTime = compileTime;
    }



    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((number == null) ? 0 : number.hashCode());
        result = prime * result + ((revision == null) ? 0 : revision.hashCode());
        return result;
    }



    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Version other = (Version) obj;
        if (number == null) {
            if (other.number != null)
                return false;
        } else if (!number.equals(other.number))
            return false;
        if (revision == null) {
            if (other.revision != null)
                return false;
        } else if (!revision.equals(other.revision))
            return false;
        return true;
    }



    @Override
    public String toString() {
        return getNumber() + " | " + getRevision() +
                " | " + getCompileTime() +
                " | by " + getCompiledBy();
    }



    public static Version readFromMy() {
        return new Version("1.4.0", "1.4.0", "ZhenQin",
                FastDateFormat.getInstance("yyyy-MM-dd")
                        .format(System.currentTimeMillis()));
    }



    public static Version readFromJar() {
        String jar = findContainingJar(Version.class);
        String number;
        String revision;
        String compiledBy;
        String compileTime;
        if (StringUtils.isNotBlank(jar)) {
            LOG.debug("load version info from '" + jar + "'");
            final Manifest manifest = getManifest(jar);
            final Attributes attrs = manifest.getMainAttributes();
            number = attrs.getValue("Implementation-Version");
            revision = attrs.getValue("Specification-Version");
            compiledBy = attrs.getValue("Built-By");
            compileTime = attrs.getValue("Built-Time");
        } else {
            LOG.warn("could not find katta jar - setting version infos to unknown");
            return readFromMy();
        }
        return new Version(number, revision, compiledBy, compileTime);
    }

    private static String findContainingJar(Class<?> clazz) {
        ClassLoader loader = clazz.getClassLoader();
        String className = clazz.getName().replaceAll("\\.", "/") + ".class";
        try {
            for (Enumeration<URL> enumeration = loader.getResources(className); enumeration.hasMoreElements(); ) {
                URL url = enumeration.nextElement();
                if ("jar".equals(url.getProtocol())) {
                    String path = url.getPath();
                    if (path.startsWith("file:")) {
                        path = path.substring("file:".length());
                    }
                    path = URLDecoder.decode(path, "UTF-8");
                    return path.replaceAll("!.*$", "");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private static Manifest getManifest(String jar) {
        try {
            final JarFile jarFile = new JarFile(jar);
            final Manifest manifest = jarFile.getManifest();
            return manifest;
        } catch (Exception e) {
            throw new RuntimeException("could not load manifest from jar " + jar);
        }
    }


    public static void main(String[] args) {
        Manifest manifest = getManifest(findContainingJar(Version.class));

        LOG.info(manifest.toString());
        LOG.info(ReflectionToStringBuilder.toString(manifest));
    }
}
