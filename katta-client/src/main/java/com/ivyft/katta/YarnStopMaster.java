package com.ivyft.katta;

import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.NodeConfiguration;
import com.ivyft.katta.util.ZkConfiguration;
import com.ivyft.katta.yarn.KattaOnYarn;
import com.ivyft.katta.yarn.protocol.KattaYarnClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;

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
public class YarnStopMaster extends ProtocolCommand {


    private String appId;

    public YarnStopMaster() {
        super("yarn-stop-master", "katta on yarn, stop master");
    }


    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
        KattaYarnClient yarnClient = KattaOnYarn.attachToApp(appId, new NodeConfiguration()).getClient();
        yarnClient.stopMaster();
        yarnClient.close();
    }

    @Override
    public Options getOpts() {
        Options options = new Options();
        options.addOption("appid", "appid", true, "App Id, KattaOnYarn ApplicationMaster ID");
        options.addOption("s", false, "print exception");
        return options;
    }

    @Override
    public void process(CommandLine cl) throws Exception {
        this.appId = cl.getOptionValue("appid");
        if(StringUtils.isBlank(appId)) {
            throw new IllegalArgumentException("app id must not be null.");
        }

        //ApplicationId.newInstance()

        execute(new ZkConfiguration());
    }
}
