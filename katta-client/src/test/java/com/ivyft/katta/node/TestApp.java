package com.ivyft.katta.node;

import com.ivyft.katta.lib.lucene.DefaultSearcherFactory;
import com.ivyft.katta.lib.lucene.LuceneServer;
import com.ivyft.katta.lib.lucene.SolrHandler;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.ClassUtil;
import com.ivyft.katta.util.NodeConfiguration;
import com.ivyft.katta.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/26
 * Time: 16:05
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class TestApp {


    public TestApp() {
    }


    public static void main(String[] args) throws Exception {
        ZkConfiguration zkConf = new ZkConfiguration();

        ZkClient zkClient = new ZkClient(zkConf.getZKServers());


        InteractionProtocol protocol = new InteractionProtocol(zkClient, zkConf);
        NodeConfiguration nodeConfiguration = new NodeConfiguration();;
        SolrHandler.init(nodeConfiguration.getFile("node.solrhome.folder"));

        LuceneServer server = new LuceneServer();
        final Node node = new Node(protocol, nodeConfiguration, server);
        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                node.shutdown();
            }
        });
        node.join();
    }
}
