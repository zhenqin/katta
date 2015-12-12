package com.ivyft.katta;

import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.NodeConfiguration;
import com.ivyft.katta.util.ZkConfiguration;
import com.ivyft.katta.yarn.KattaOnYarn;
import com.ivyft.katta.yarn.protocol.KattaAndNode;
import com.ivyft.katta.yarn.protocol.KattaYarnClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/11
 * Time: 13:37
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class YarnListNodes extends ProtocolCommand {


    private String appId;


    private String m = MASTER;


    public YarnListNodes() {
        super("yarn-list", "katta on yarn, List Masters or Nodes");
    }


    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
        KattaYarnClient yarnClient = KattaOnYarn.attachToApp(appId, new NodeConfiguration()).getClient();
        if(StringUtils.equals(m, MASTER)) {
            //list masters
            List<KattaAndNode> kattaAndNodes = yarnClient.listMasters();
            for (KattaAndNode kattaAndNode : kattaAndNodes) {
                System.out.println(kattaAndNode);
            }
        } else {
            //list nodes
            List<KattaAndNode> kattaAndNodes = yarnClient.listNodes();
            for (KattaAndNode kattaAndNode : kattaAndNodes) {
                System.out.println(kattaAndNode);
            }
        }
        yarnClient.close();
    }

    @Override
    public Options getOpts() {
        Options options = new Options();
        options.addOption("appid", "appid", true, "App Id, KattaOnYarn ApplicationMaster ID");
        options.addOption("master", "master", false, "Katta Master");
        options.addOption("node", "node", false, "Katta Node");
        options.addOption("s", false, "print exception");
        return options;
    }

    @Override
    public void process(CommandLine cl) throws Exception {
        this.appId = cl.getOptionValue("appid");
        if(StringUtils.isBlank(appId)) {
            throw new IllegalArgumentException("app id must not be null.");
        }

        ApplicationId applicationId = ConverterUtils.toApplicationId(appId);
        System.out.println(applicationId);

        if(cl.hasOption(MASTER)) {
            m = MASTER;
        } else if(cl.hasOption(NODE)) {
            m = NODE;
        } else {
            throw new IllegalArgumentException("unknown option. -master or -node");
        }

        execute(new ZkConfiguration());
    }
}
