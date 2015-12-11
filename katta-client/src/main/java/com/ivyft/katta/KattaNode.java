package com.ivyft.katta;

import com.ivyft.katta.lib.lucene.SolrHandler;
import com.ivyft.katta.node.IContentServer;
import com.ivyft.katta.node.Node;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.net.SyslogAppender;

import java.io.File;
import java.net.URI;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/11
 * Time: 16:17
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaNode extends ProtocolCommand {
    public KattaNode() {
        super("node", "Starts a local node");
    }


    private NodeConfiguration nodeConfiguration;
    private IContentServer server = null;

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
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
                CompressUtils.uncompressZip(new Path(solrHome), HadoopUtil.getFileSystem(), solrHomeDir);
                SolrHandler.init(solrHomeDir);
            } else {
                SolrHandler.init(new File(solrHome));
            }
        } else {
            //katta standalone
            File file = nodeConfiguration.getFile("node.solrhome.folder");
            SolrHandler.init(file);
        }


        final Node node = new Node(protocol, nodeConfiguration, server);

        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                node.shutdown();
            }
        });
        node.join();
    }

    @Override
    public Options getOpts() {
        Options options = new Options();
        options.addOption("c", false, "node server class, ? implements ILuceneServer");
        options.addOption("p", false, "port number");
        options.addOption("s", false, "print exception");
        return options;
    }

    @Override
    public void process(CommandLine cl) throws Exception {
        nodeConfiguration = new NodeConfiguration();
        String serverClassName;
        if(cl.hasOption("c")) {
            serverClassName = cl.getOptionValue("c");
        } else {
            serverClassName = nodeConfiguration.getServerClassName();
        }
        if (cl.hasOption("p")) {
            String portNumber = cl.getOptionValue("p");
            nodeConfiguration.setStartPort(Integer.parseInt(portNumber));
        }

        Class<?> serverClass = ClassUtil.forName(serverClassName, IContentServer.class);
        server = (IContentServer) ClassUtil.newInstance(serverClass);

        execute(new ZkConfiguration());
    }
}
