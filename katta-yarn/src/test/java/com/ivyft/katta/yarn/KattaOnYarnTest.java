package com.ivyft.katta.yarn;

import com.ivyft.katta.util.KattaConfiguration;
import com.ivyft.katta.yarn.protocol.KattaAndNode;
import com.ivyft.katta.yarn.protocol.KattaYarnClient;
import org.junit.Test;

import java.util.List;

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


    String appId = "application_1449896461425_0013";



    public KattaOnYarnTest() {
    }



    @Test
    public void testStartMaster() throws Exception {
        KattaYarnClient client = KattaOnYarn.attachToApp(appId,
                new KattaConfiguration("katta.node.properties")).getClient();
        client.startMaster(512, 1, null);
        client.close();
    }



    @Test
    public void testListMaster() throws Exception {
        KattaYarnClient client = KattaOnYarn.attachToApp(appId,
                new KattaConfiguration("katta.node.properties")).getClient();
        List<KattaAndNode> kattaAndNodes = client.listMasters();
        for (KattaAndNode kattaAndNode : kattaAndNodes) {
            System.out.println(kattaAndNode);
        }
        client.close();
    }


    @Test
    public void testStopMaster() throws Exception {
        KattaYarnClient client = KattaOnYarn.attachToApp(appId,
                new KattaConfiguration("katta.node.properties")).getClient();
        List<KattaAndNode> kattaAndNodes = client.listMasters();
        for (KattaAndNode kattaAndNode : kattaAndNodes) {
            System.out.println(kattaAndNode);
            //client.stopMaster(kattaAndNode);
        }
        client.close();
    }


    @Test
    public void testStartNode() throws Exception {
        KattaYarnClient client = KattaOnYarn.attachToApp(appId,
                new KattaConfiguration("katta.node.properties")).getClient();
        client.addNode(512, 1, null, null);
        client.close();
    }


    @Test
    public void testShutdown() throws Exception {
        KattaYarnClient client = KattaOnYarn.attachToApp(appId,
                new KattaConfiguration("katta.node.properties")).getClient();
        client.shutdown();
        client.close();
    }
}
