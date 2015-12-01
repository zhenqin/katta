package com.ivyft.katta.yarn;

import com.ivyft.katta.util.KattaConfiguration;
import com.ivyft.katta.yarn.protocol.KattaYarnClient;
import org.junit.Test;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/1
 * Time: 13:16
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaOnYarnTest {


    public KattaOnYarnTest() {
    }



    @Test
    public void testStartMaster() throws Exception {
        KattaYarnClient client = KattaOnYarn.attachToApp("application_1448967468356_0006",
                new KattaConfiguration("katta.node.properties")).getClient();
        client.startMaster(1);
        client.close();
    }


    @Test
    public void testShutdown() throws Exception {
        KattaYarnClient client = KattaOnYarn.attachToApp("application_1448967468356_0005",
                new KattaConfiguration("katta.node.properties")).getClient();
        client.shutdown();
        client.close();
    }
}
