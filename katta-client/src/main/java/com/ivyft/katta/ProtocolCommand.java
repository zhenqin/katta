package com.ivyft.katta;

import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 14-3-22
 * Time: 下午1:44
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public abstract class ProtocolCommand extends Command {



    public final static String MASTER = "master";


    public final static String NODE = "node";



    public ProtocolCommand(String command, String description) {
        super(command, description);
    }

    @Override
    public final void execute(ZkConfiguration zkConf) throws Exception {
        ZkClient zkClient = new ZkClient(zkConf.getZKServers());
        InteractionProtocol protocol = new InteractionProtocol(zkClient, zkConf);
        execute(zkConf, protocol);
        protocol.disconnect();
    }

    protected abstract void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception;
}
