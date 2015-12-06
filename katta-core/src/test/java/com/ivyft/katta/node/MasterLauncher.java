package com.ivyft.katta.node;

import com.ivyft.katta.lib.lucene.LuceneServer;
import com.ivyft.katta.lib.lucene.SolrHandler;
import com.ivyft.katta.master.Master;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.MasterConfiguration;
import com.ivyft.katta.util.NodeConfiguration;
import com.ivyft.katta.util.ZkConfiguration;
import com.ivyft.katta.util.ZkKattaUtil;
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
public class MasterLauncher {


    public MasterLauncher() {
    }


    public static void main(String[] args) throws Exception {
        ZkConfiguration zkConf = new ZkConfiguration();

        ZkClient zkClient = new ZkClient(zkConf.getZKServers());


        InteractionProtocol protocol = new InteractionProtocol(zkClient, zkConf);
        MasterConfiguration conf = new MasterConfiguration();;

        final Master master = new Master(protocol, true, conf);
        master.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                master.shutdown();
            }
        });

        synchronized (master) {
            master.wait();
        }
    }
}
