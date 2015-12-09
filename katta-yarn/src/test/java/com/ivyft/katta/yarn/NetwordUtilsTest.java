package com.ivyft.katta.yarn;

import com.google.common.net.InetAddresses;
import com.ivyft.katta.util.NetworkUtils;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/9
 * Time: 10:34
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class NetwordUtilsTest {


    @Test
    public void testPrintName() throws Exception {
        System.out.println(NetworkUtils.getLocalhostName());
        String target = "0.0.0.0:8080";
        System.out.println(NetUtils.createSocketAddr(target).getHostName());

        target = "127.0.0.1:8080";
        System.out.println(NetUtils.createSocketAddr(target).getHostName());

        target = "zhenqin-pro102:8080";
        System.out.println(NetUtils.createSocketAddr(target).getHostName());

        System.out.println("==========================================");
        System.out.println(InetAddresses.forString("192.168.102.10").getHostName());
        System.out.println(InetAddresses.forString("0.0.0.0").getHostName());
        System.out.println(InetAddresses.forString("127.0.0.1").getHostName());

        System.out.println(InetAddress.getByName("zhenqin-pro102").getHostName());
        System.out.println(InetAddress.getByName("zhenqin-pro102").getHostAddress());
    }
}
