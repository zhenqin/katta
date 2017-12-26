package com.ivyft.katta.node;

import com.ivyft.katta.client.INodeProxyManager;
import com.ivyft.katta.client.NodeProxyManager;
import com.ivyft.katta.client.ShuffleNodeSelectionPolicy;
import com.ivyft.katta.lib.lucene.FieldInfoWritable;
import com.ivyft.katta.lib.lucene.ILuceneServer;
import com.ivyft.katta.util.HadoopUtil;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 2017/12/22
 * Time: 09:44
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaRpcTest {

    protected INodeProxyManager<ILuceneServer> proxyManager;


    @Before
    public void setUp() throws Exception {
        proxyManager = new NodeProxyManager<>(ILuceneServer.class,
                HadoopUtil.getHadoopConf(), new ShuffleNodeSelectionPolicy());
    }


    @Test
    public void testGetFieldsInfo() throws Exception {
        ILuceneServer server = proxyManager.getProxy("localhost:20000", true);

        FieldInfoWritable fieldsInfo = server.getFieldsInfo("userindex#userindex#gMyTchetOSn3ql8fkBZ");

        System.out.println(fieldsInfo.getShard());

        List<List<Object>> result = fieldsInfo.getResult();
        for (List<Object> objects : result) {
            System.out.println(objects);
        }
    }
}
