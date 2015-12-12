package com.ivyft.katta;

import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.NodeConfiguration;
import com.ivyft.katta.util.ZkConfiguration;
import com.ivyft.katta.yarn.KattaOnYarn;
import com.ivyft.katta.yarn.protocol.IdType;
import com.ivyft.katta.yarn.protocol.KattaYarnClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

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
public class YarnStopNodes extends ProtocolCommand {


    private String appId;


    private String id;


    private IdType idType = IdType.CONTAINER_ID;



    private String module = "single";



    private String m = MASTER;



    public YarnStopNodes() {
        super("yarn-stop", "katta on yarn, stop katta master or node");
    }


    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
        KattaYarnClient yarnClient = KattaOnYarn.attachToApp(appId, new NodeConfiguration()).getClient();

        if(StringUtils.equals(m, MASTER)) {
            //stop katta master
            if(StringUtils.equals("all", module)) {
                yarnClient.stopAllMaster();
            } else {
                yarnClient.stopMaster(id, idType);
            }
        } else {
            //stop katta node
            if(StringUtils.equals("all", module)) {
                yarnClient.stopAllNode();
            } else {
                yarnClient.stopNode(id, idType);
            }
        }
        yarnClient.close();
    }

    @Override
    public Options getOpts() {
        Options options = new Options();
        options.addOption("appid", "appid", true, "App Id, KattaOnYarn ApplicationMaster ID");
        options.addOption("c", "containerid", true, "Container ID");
        options.addOption("n", "nodeid", true, "Node Id, Yarn Node ID");

        options.addOption("a", "all", false, "all, shutdown all master.");
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

        if(cl.hasOption("a")) {
            module = "all";
            execute(new ZkConfiguration());
            return;
        }

        if(cl.hasOption("c")) {
            this.id = cl.getOptionValue("c");
            if(StringUtils.isBlank(id)) {
                throw new IllegalArgumentException("containerid or nodeid must not be null.");
            }
        } else if(cl.hasOption("n")) {
            idType = IdType.NODE_ID;
            this.id = cl.getOptionValue("n");
            if(StringUtils.isBlank(id)) {
                throw new IllegalArgumentException("containerid or nodeid must not be null.");
            }
        } else {
            throw new IllegalArgumentException("unknown params, containerid or nodeid must not be null.");
        }
        execute(new ZkConfiguration());
    }
}
