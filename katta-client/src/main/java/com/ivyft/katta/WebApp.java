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
package com.ivyft.katta;


import java.lang.reflect.Constructor;
import java.lang.reflect.Method;


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
     * UI Booster 的 Class 实例
     */
    private final Class instanceClass;


    /**
     * Booster 实例
     */
    private final Object instance;

    /**
     * 启动一个web项目
     *
     * @param host Host
     * @param port 端口号
     */
    public WebApp(String host, int port) {
        try {
            instanceClass = Class.forName("com.ivyft.katta.ui.Booster");
            Constructor constructor = instanceClass.getDeclaredConstructor(String.class, Integer.TYPE);
            instance = constructor.newInstance(host, port);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }


    /**
     * 启动web服务器,启动成功后，线程阻塞。 停止调用 {@link #stopWebServer()}
     *
     * @throws Exception
     */
    public void startWebServer() throws Exception {
        Method startMethod = instanceClass.getDeclaredMethod("start");
        startMethod.invoke(instance);

        Method serveMethod = instanceClass.getDeclaredMethod("serve");
        serveMethod.invoke(instance);
    }


    /**
     * 停止 Web 服务器
     * @throws Exception
     */
    public void stopWebServer() throws Exception {
        Method stop = instanceClass.getDeclaredMethod("stop");
        stop.invoke(instance);
    }

    public static void main(String[] args) throws Exception {
        WebApp webApp = new WebApp(args[0], Integer.parseInt(args[1]));
        webApp.startWebServer();
    }

}
