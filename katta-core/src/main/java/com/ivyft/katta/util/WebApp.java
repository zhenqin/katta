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
package com.ivyft.katta.util;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.webapp.WebAppContext;

import java.io.File;


/**
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
public class WebApp {


    /**
     * Web项目WAR地址
     */
    private final String warPath;

    /**
     * Web端口号
     */
    private int port;


    /**
     * 启动一个web项目
     *
     * @param paths 多个Path的搜索路径，下边要至少包含一个war文件
     * @param port 端口号
     */
    public WebApp(String[] paths, int port) {
        this.warPath = findWarInPathOrChilds(paths);
        this.port = port;
    }


    /**
     * 查找warPaths目录下的地一个war文件。找到即刻返回。
     * @param warPaths 多个文件路径
     * @return 返回找到的war文件的绝对路径
     */
    private String findWarInPathOrChilds(String[] warPaths) {
        for (String path : warPaths) {
            File file = new File(path);
            if (isWarFile(file)) {
                return file.getAbsolutePath();
            }
            File[] listFiles = file.listFiles();
            if (listFiles != null) {
                for (File subFiles : listFiles) {
                    if (isWarFile(subFiles)) {
                        return subFiles.getAbsolutePath();
                    }
                }
            }
        }
        throw new IllegalArgumentException("Unable to find war");
    }

    private boolean isWarFile(File file) {
        return file.exists() && file.getName().endsWith(".war");
    }


    /**
     * 启动web服务器
     *
     * @throws Exception
     */
    public void startWebServer() throws Exception {

        Server server = new Server();
        Connector connector = new SelectChannelConnector();
        connector.setPort(this.port);
        server.setConnectors(new Connector[]{connector});

        WebAppContext webapp = new WebAppContext();
        webapp.setContextPath("/");
        webapp.setWar(this.warPath);
        server.setHandler(webapp);

        server.start();
        server.join();
    }

    public static void main(String[] args) throws Exception {
        WebApp webApp = new WebApp(new String[]{args[0]}, Integer.parseInt(args[1]));
        webApp.startWebServer();
    }

}
