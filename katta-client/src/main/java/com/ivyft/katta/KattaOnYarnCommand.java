package com.ivyft.katta;

import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.NodeConfiguration;
import com.ivyft.katta.util.ZkConfiguration;
import com.ivyft.katta.yarn.KattaOnYarn;
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
public class KattaOnYarnCommand extends ProtocolCommand {


    private String appName = "KattaOnYarn";
    private String queue = "default";
    private int amMB = 512;
    private String kattaZip;

    public KattaOnYarnCommand() {
        super("yarn", "katta on yarn, start katta on yarn app master");
    }


    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
        KattaOnYarn.launchApplication(appName, queue, amMB, new NodeConfiguration(), kattaZip);
    }

    @Override
    public Options getOpts() {
        Options options = new Options();
        options.addOption("n", "appname", false, "App Name, KattaOnYarn");
        options.addOption("q", "queue", false, "Hadoop Yarn Queue, default");
        options.addOption("m", "am-mb", false, "ApplicationMaster Memory, default 512M");
        options.addOption("z", "zip", false, "Katta Zip Location, default /lib/katta/katta-{version}.zip");
        options.addOption("s", false, "print exception");
        return options;
    }

    @Override
    public void process(CommandLine cl) throws Exception {
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
