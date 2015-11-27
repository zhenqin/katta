package com.ivyft.katta.node;

import com.ivyft.katta.client.KattaClient;
import com.ivyft.katta.client.KattaLoader;
import org.junit.Test;

import java.util.Random;

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
        KattaLoader<String> loader = new KattaClient<String>("localhost", 7690);
        for (int i = 0; i < 10; i++) {
            System.out.println(loader.addBean("java" + i, "hello" + new Random().nextInt()));
        }
        loader.commit();
    }
}
