package com.ivyft.katta.node;

import com.ivyft.katta.client.DataLoaderProxy;
import com.ivyft.katta.client.KattaLoader;
import org.junit.Test;

import java.lang.reflect.Proxy;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/27
 * Time: 20:54
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaLoaderTest {


    public KattaLoaderTest() {

    }


    @Test
    public void testSend() throws Exception {
        KattaLoader<Object> loader = get("localhost", 7690);
        System.out.println(loader.addBean("java", "hello"));

    }

    public static <T> KattaLoader<T> get(String host, int port) {
        return  (KattaLoader<T>) Proxy.newProxyInstance(
                KattaLoader.class.getClassLoader(),
                new Class[]{KattaLoader.class},
                new DataLoaderProxy(host, port));

    }
}
