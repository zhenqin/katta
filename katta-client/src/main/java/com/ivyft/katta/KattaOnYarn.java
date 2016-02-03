package com.ivyft.katta;

import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.KattaConfiguration;
import com.ivyft.katta.util.NodeConfiguration;
import com.ivyft.katta.util.ZkConfiguration;
import com.ivyft.katta.yarn.protocol.KattaYarnClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;

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
public class KattaOnYarn extends ProtocolCommand {


    private String appName = "KattaOnYarn";
    private String appId;
    private String queue = "default";
    private int amMB = 512;
    private String kattaZip;

    public KattaOnYarn() {
        super("yarn", "katta on yarn, start/shutdown katta on yarn app master");
    }


    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
        if(appId != null) {
            KattaYarnClient client = com.ivyft.katta.yarn.KattaOnYarn.attachToApp(appId,
                    new KattaConfiguration("katta.node.properties")).getClient();
            client.shutdown();
            client.close();
        } else {
            com.ivyft.katta.yarn.KattaOnYarn.launchApplication(appName, queue, amMB, new NodeConfiguration(), kattaZip);
        }
    }

    @Override
    public Options getOpts() {
        Options options = new Options();
        options.addOption("appid", "appid", true, "App Id, KattaOnYarn ApplicationMaster ID");
        options.addOption("shutdown", "shutdown", false, "App Name, shutdown KattaOnYarn");
        options.addOption("n", "appname", true, "App Name, KattaOnYarn");
        options.addOption("q", "queue", true, "Hadoop Yarn Queue, default");
        options.addOption("m", "am-mb", true, "ApplicationMaster Memory, default 512M");
        options.addOption("z", "zip", true, "Katta Zip Location, default /lib/katta/katta-{version}.zip");
        options.addOption("s", false, "print exception");
        return options;
    }

    @Override
    public void process(CommandLine cl) throws Exception {
        if(cl.hasOption("shutdown")) {
            this.appId = cl.getOptionValue("appid");
            if(StringUtils.isBlank(appId)) {
                throw new IllegalArgumentException("app id must not be null.");
            }

            ApplicationId applicationId = ConverterUtils.toApplicationId(appId);
            System.out.println(applicationId);
            execute(new ZkConfiguration());
            return;
        }
        System.out.println("starting katta on yarn, input Y/N");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String s = reader.readLine();
        if(!StringUtils.equals("Y", StringUtils.upperCase(s))) {
            return;
        }
        String appName = cl.getOptionValue("n");
        if(StringUtils.isNotBlank(appName)) {
            this.appName = appName;
        }

        String queue = cl.getOptionValue("q");
        if(StringUtils.isNotBlank(queue)) {
            this.queue = queue;
        }

        String m = cl.getOptionValue("m");
        if(StringUtils.isNotBlank(m)) {
            this.amMB = Integer.parseInt(m);
        }

        String kattaZip = cl.getOptionValue("z");
        if(StringUtils.isNotBlank(kattaZip)) {
            this.kattaZip = kattaZip;
        }

        execute(new ZkConfiguration());
    }
}
