package com.ivyft.katta.node;

import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/6
 * Time: 21:35
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class InteractionProtocolTest {

    @Test
    public void testPrintMasters() throws Exception {
        ZkConfiguration zkConf = new ZkConfiguration();
        ZkClient zkClient = new ZkClient(zkConf.getZKServers());

        InteractionProtocol protocol = new InteractionProtocol(zkClient, zkConf);
        System.out.println(protocol.getMasters());

    }
}
