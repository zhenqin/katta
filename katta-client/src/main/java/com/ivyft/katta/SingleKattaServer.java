package com.ivyft.katta;

import com.ivyft.katta.lib.lucene.SolrHandler;
import com.ivyft.katta.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

import java.io.File;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/2/26
 * Time: 21:19
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class SingleKattaServer extends Command {


    /**
     * conf
     */
    private NodeConfiguration nodeConfiguration;




    public SingleKattaServer() {
        super("server", "Start Single Katta Server");
    }



    @Override
    public void execute(ZkConfiguration zkConf) throws Exception {
        //toRet.add("-Dsolr.solr.home=" + solrHome);
        //toRet.add("-Dnode.solrhome.folder=" + solrHome);

        String solrHome = System.getProperty("solr.solr.home");
        if(StringUtils.isBlank(solrHome)) {
            solrHome = System.getProperty("node.solrhome.folder");
        }

        if(StringUtils.isNotBlank(solrHome)) {
            //katta on yarn
            if(solrHome.startsWith("hdfs://")) {
                //solr home on hdfs, or hdfs zip file
                String pwd = System.getProperty("user.dir");
                if(StringUtils.isBlank(pwd)) {
                    pwd = ".";
                }

                Path kattaHome = new Path(pwd, "solr");
                File solrHomeDir = new File(kattaHome.toString());
                CompressUtils.uncompressZip(new Path(solrHome),
                        HadoopUtil.getFileSystem(), solrHomeDir);

                System.setProperty("solr.solr.home", solrHomeDir.getAbsolutePath());
                System.setProperty("node.solrhome.folder", solrHomeDir.getAbsolutePath());
                SolrHandler.init(solrHomeDir);
            } else {
                SolrHandler.init(new File(solrHome));
            }
        } else {
            //katta standalone
            File file = nodeConfiguration.getFile("node.solrhome.folder");
            SolrHandler.init(file);
        }


        com.ivyft.katta.server.lucene.KattaLuceneServer server = new com.ivyft.katta.server.lucene.KattaLuceneServer();
        final com.ivyft.katta.server.KattaBooter booter = new com.ivyft.katta.server.KattaBooter(nodeConfiguration, server);
        booter.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                booter.shutdown();
            }
        });
        booter.serve();
    }

    @Override
    public Options getOpts() {
        Options options = new Options();
        options.addOption("p", true, "port number");
        options.addOption("s", false, "print exception");
        return options;
    }

    @Override
    public void process(CommandLine cl) throws Exception {
        nodeConfiguration = new NodeConfiguration();
        if (cl.hasOption("p")) {
            String portNumber = cl.getOptionValue("p");
            nodeConfiguration.setProperty(com.ivyft.katta.server.KattaBooter.KATTA_SERVER_PORT, Integer.parseInt(portNumber));
        }

        execute(new ZkConfiguration());
    }
}
