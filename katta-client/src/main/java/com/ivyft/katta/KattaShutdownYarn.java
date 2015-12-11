package com.ivyft.katta;

import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.KattaConfiguration;
import com.ivyft.katta.util.ZkConfiguration;
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
public class KattaShutdownYarn extends ProtocolCommand {


    private String appId;

    public KattaShutdownYarn() {
        super("stop-yarn", "katta on yarn, stop katta on yarn, stop app master");
    }


    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
        KattaYarnClient client = com.ivyft.katta.yarn.KattaOnYarn.attachToApp(appId,
                new KattaConfiguration("katta.node.properties")).getClient();
        client.shutdown();
        client.close();
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

        ApplicationId applicationId = ConverterUtils.toApplicationId(appId);
        System.out.println(applicationId);


        execute(new ZkConfiguration());
    }
}
