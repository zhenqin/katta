package com.ivyft.katta;

import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.NodeConfiguration;
import com.ivyft.katta.util.ZkConfiguration;
import com.ivyft.katta.yarn.KattaOnYarn;
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
public class YarnStartMaster extends ProtocolCommand {


    private String appId;

    private int masterMB = 512;


    private String kattaZip;

    public YarnStartMaster() {
        super("yarn-start-master", "katta on yarn, start katta master");
    }


    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
        KattaYarnClient yarnClient = KattaOnYarn.attachToApp(appId, new NodeConfiguration()).getClient();
        yarnClient.startMaster(masterMB, 1, kattaZip);
    }

    @Override
    public Options getOpts() {
        Options options = new Options();
        options.addOption("i", "appid", true, "App Id, KattaOnYarn ApplicationMaster ID");
        options.addOption("m", "memory", false, "Katta Node Memory, default 512M");
        options.addOption("z", "zip", false, "Katta Zip Location, default /lib/katta/katta-{version}.zip");
        options.addOption("s", false, "print exception");
        return options;
    }

    @Override
    public void process(CommandLine cl) throws Exception {
        this.appId = cl.getOptionValue("i");
        if(StringUtils.isNotBlank(appId)) {
            throw new IllegalArgumentException("app id must not be null.");
        }

        ApplicationId applicationId = ConverterUtils.toApplicationId(appId);
        System.out.println(applicationId);


        String m = cl.getOptionValue("m");
        if(StringUtils.isNotBlank(m)) {
            this.masterMB = Integer.parseInt(m);
        }

        String kattaZip = cl.getOptionValue("z");
        if(StringUtils.isNotBlank(kattaZip)) {
            this.kattaZip = kattaZip;
        }

        execute(new ZkConfiguration());
    }
}