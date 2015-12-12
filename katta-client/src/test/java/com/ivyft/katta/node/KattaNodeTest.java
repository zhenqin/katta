package com.ivyft.katta.node;

import com.ivyft.katta.Katta;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/11
 * Time: 17:43
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaNodeTest {


    @Test
    public void testGnuArgs() throws Exception {
        String[] commandArgs = new String[]{"-c", "jx", "-m", "500"};
        Options options = new Options();
        options.addOption("m", true, "Katta Node Memory, default 512M");
        options.addOption("c", true, "Katta Node Cores, default 1");
        //options.addOption("f", "ffff", false, "Solr Home Location, default /lib/solr/solr.zip");
        //options.addOption("z", "zip", false, "Katta Zip Location, default /lib/katta/katta-{version}.zip");
        //options.addOption("master", "master", false, "Katta Master");
        //options.addOption("node", "node", false, "Katta Node");
        //options.addOption("s", false, "print exception");

        CommandLine cl = new GnuParser().parse(options, commandArgs);



        System.out.println(cl.getArgList());
        System.out.println(cl.getOptionValue("m"));
        System.out.println(cl.getOptionValue("c"));

    }

    @Test
    public void testStartNode() throws Exception {
        Katta.main(new String[]{
                "yarn-start", "-master",
                "-appid", "application_1449896461425_0014"});
    }
}
