package com.ivyft.katta.presto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.List;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 17/12/13
 * Time: 17:36
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class ClassInJarUtils {
    private Logger logger = LoggerFactory.getLogger(ClassInJarUtils.class);

    private String basePackage;
    private ClassLoader cl;

    /**
     * Construct an instance and specify the base package it should scan.
     * @param basePackage The base package to scan.
     */
    public ClassInJarUtils(String basePackage) {
        this.basePackage = basePackage;
        this.cl = getClass().getClassLoader();

    }

    /**
     * Construct an instance with base package and class loader.
     * @param basePackage The base package to scan.
     * @param cl Use this class load to locate the package.
     */
    public ClassInJarUtils(String basePackage, ClassLoader cl) {
        this.basePackage = basePackage;
        this.cl = cl;
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
                toReturn = toReturn.replaceAll("\\+", "%2B");
                toReturn = URLDecoder.decode(toReturn, "UTF-8");
                return toReturn.replaceAll("!.*$", "");
            } else if("file".equals(url.getProtocol())) {
                String pack = my_class.getName().replaceAll("\\.", "/") + ".class";
                return url.getPath().replace(pack, "");
            }
        }
        throw new IOException("Fail to locat a JAR for class: "+my_class.getName());
    }


    /**
     * For test purpose.
     */
    public static void main(String[] args) throws Exception {
        System.out.println(ClassInJarUtils.findContainingJar(ClassInJarUtils.class));
    }

}
