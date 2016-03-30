/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ivyft.katta;

import com.ivyft.katta.client.*;
import com.ivyft.katta.lib.lucene.*;
import com.ivyft.katta.master.Master;
import com.ivyft.katta.node.IContentServer;
import com.ivyft.katta.node.monitor.MetricLogger;
import com.ivyft.katta.node.monitor.MetricLogger.OutputType;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.ReplicationReport;
import com.ivyft.katta.protocol.metadata.*;
import com.ivyft.katta.tool.SampleIndexGenerator;
import com.ivyft.katta.util.*;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.*;

/**
 *
 * <p>
 *
 * Provides command line access to a Katta cluster.
 *
 * </p>
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-13
 * Time: 上午8:58
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 *
 */
public class Katta {

    protected static final Logger log = LoggerFactory.getLogger(Katta1.class);

    protected final static HashMap<String, Command> commands = new HashMap<String, Command>();


    protected final static Command help = new Command("help", "print out this message") {


        @Override
        public Options getOpts() {
            Options options = new Options();
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            printHelpFor(cl.getArgList());
        }

        @Override
        protected void execute(ZkConfiguration zkConf) throws Exception {
        }
    };


    protected static Command START_ZK_COMMAND = new Command("startZk", "Starts a local zookeeper server") {

        @Override
        public Options getOpts() {
            Options options = new Options();
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            execute(new ZkConfiguration());
        }


        @Override
        public void execute(ZkConfiguration zkConf) throws Exception {
            final ZkServer zkServer = ZkKattaUtil.startZkServer(zkConf);
            synchronized (zkServer) {
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    public void run() {
                        synchronized (zkServer) {
                            System.out.println("stopping zookeeper server...");
                            zkServer.shutdown();
                            zkServer.notifyAll();
                        }
                    }
                });
                System.out.println("zookeeper server started on port " +
                        zkServer.getPort());
                zkServer.wait();
            }
        }
    };



    protected static Command START_MASTER_COMMAND = new Command("master",
            "Starts a local master.") {

        private boolean embeddedMode = false;


        @Override
        public Options getOpts() {
            Options options = new Options();
            options.addOption("e", false, "embedded zk-server (overriding configuration).");
            options.addOption("ne", false, "non-embedded zk-server (overriding configuration).");
            options.addOption("config", "config", true, "node server class, ? implements ILuceneServer");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            if(cl.hasOption("e")) {
                embeddedMode = Boolean.parseBoolean(cl.getOptionValue("e"));
            }

            if(cl.hasOption("ne")) {
                embeddedMode = !Boolean.parseBoolean(cl.getOptionValue("ne"));
            }

            execute(new ZkConfiguration());
        }

        @Override
        public void execute(ZkConfiguration zkConf) throws Exception {
            final Master master;
            if (embeddedMode) {
                log.info("starting embedded zookeeper server...");
                ZkServer zkServer = ZkKattaUtil.startZkServer(zkConf);
                log.info("started embedded zookeeper server success.");

                log.info("try to start master...");
                master = new Master(new InteractionProtocol(zkServer.getZkClient(), zkConf), zkServer);
            } else {
                ZkClient zkClient = ZkKattaUtil.startZkClient(zkConf, 30000);

                log.info("try to start master...");
                master = new Master(new InteractionProtocol(zkClient, zkConf), true);
            }
            master.start();

            synchronized (master) {
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    public void run() {
                        synchronized (master) {
                            master.shutdown();
                            master.notifyAll();
                        }
                    }
                });
                master.wait();
            }
        }
    };


    protected static Command LIST_NODES_COMMAND = new ProtocolCommand("listNodes",
            "Lists all nodes. ") {

        private boolean batchMode;
        private boolean skipColumnNames;
        private boolean sorted;

        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) {
            final List<String> knownNodes = protocol.getKnownNodes();
            final List<String> liveNodes = protocol.getLiveNodes();
            final Table table = new Table();
            table.setBatchMode(batchMode);
            table.setSkipColumnNames(skipColumnNames);
            if (sorted) {
                Collections.sort(knownNodes);
            }
            int numNodes = 0;
            for (final String node : knownNodes) {
                numNodes++;
                NodeMetaData nodeMetaData = protocol.getNodeMD(node);
                table.addRow(nodeMetaData.getName(), nodeMetaData.startTimeAsDate(), liveNodes.contains(node) ? "CONNECTED"
                        : "DISCONNECTED");
            }
            table.setHeader("Name (" + liveNodes.size() + "/" + knownNodes.size() + " connected)", "Start time", "State");
            System.out.println(table.toString());
        }

        @Override
        public Options getOpts() {
            Options options = new Options();
            //"[-d] [-b] [-n] [-S]",
            options.addOption("b", false, "for batch mode.");
            options.addOption("n", false, "don't write column headers.");
            options.addOption("S", false, "for sorting the node names.");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            if(cl.hasOption("b")) {
                batchMode = Boolean.parseBoolean(cl.getOptionValue("b"));
            }
            if(cl.hasOption("n")) {
                skipColumnNames = Boolean.parseBoolean(cl.getOptionValue("n"));
            }
            if(cl.hasOption("S")) {
                sorted = Boolean.parseBoolean(cl.getOptionValue("S"));
            }

            execute(new ZkConfiguration());
        }
    };



    protected static Command LIST_INDICES_COMMAND = new ProtocolCommand(
            "listIndices",
            "Lists all indices. ") {

        private boolean detailedView;
        private boolean batchMode;
        private boolean skipColumnNames;
        private boolean sorted;

        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) {
            final Table table;
            if (!detailedView) {
                table = new Table(new String[]{"Name", "Status", "Replication State", "Path", "Shards", "Entries",
                        "Disk Usage"});
            } else {
                table = new Table(new String[]{"Name", "Status", "Replication State", "Path", "Shards", "Entries",
                        "Disk Usage", "Replication Count"});
            }
            table.setBatchMode(batchMode);
            table.setSkipColumnNames(skipColumnNames);

            List<String> indices = protocol.getIndices();
            if (sorted) {
                Collections.sort(indices);
            }
            for (final String index : indices) {
                final IndexMetaData indexMD = protocol.getIndexMD(index);
                Set<Shard> shards = indexMD.getShards();
                String entries = "n/a";
                if (!indexMD.hasDeployError()) {
                    entries = "" + calculateIndexEntries(shards);
                }
                long indexBytes = calculateIndexDiskUsage(indexMD.getPath());
                String state = "DEPLOYED";
                String replicationState = "BALANCED";
                if (indexMD.hasDeployError()) {
                    state = "ERROR";
                    replicationState = "-";
                } else {
                    ReplicationReport report = protocol.getReplicationReport(indexMD);
                    if (report.isUnderreplicated()) {
                        replicationState = "UNDERREPLICATED";
                    } else if (report.isOverreplicated()) {
                        replicationState = "OVERREPLICATED";
                    }

                }
                if (!detailedView) {
                    table.addRow(index, state, replicationState, indexMD.getPath(), shards.size(), entries, indexBytes);
                } else {
                    table.addRow(index, state, replicationState, indexMD.getPath(), shards.size(), entries, indexBytes,
                            indexMD.getReplicationLevel());
                }
            }
            if (!indices.isEmpty()) {
                System.out.println(table.toString());
            }
            if (!batchMode) {
                System.out.println(indices.size() + " registered indices");
                System.out.println();
            }
        }

        private int calculateIndexEntries(Set<Shard> shards) {
            int docCount = 0;
            for (Shard shard : shards) {
                Map<String, String> metaData = shard.getMetaDataMap();
                if (metaData != null) {
                    try {
                        docCount += Integer.parseInt(metaData.get(IContentServer.SHARD_SIZE_KEY));
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                }
            }
            return docCount;
        }

        private long calculateIndexDiskUsage(String index) {
            Path indexPath = new Path(index);
            URI indexUri = indexPath.toUri();
            try {
                FileSystem fileSystem = FileSystem.get(indexUri, HadoopUtil.getHadoopConf());
                if (!fileSystem.exists(indexPath)) {
                    return -1;
                }
                return fileSystem.getContentSummary(indexPath).getLength();
            } catch (Exception e) {
                return -1;
            }
        }

        @Override
        public Options getOpts() {
            //[-d] [-b] [-n] [-S]
            Options options = new Options();
            options.addOption("d", true, "for detailed view.");
            options.addOption("b", true, "for batch mode.");
            options.addOption("n", true, "don't write column headers.");
            options.addOption("S", true, "for sorting the shard names.");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            if(cl.hasOption("b")) {
                batchMode = Boolean.parseBoolean(cl.getOptionValue("b"));
            }
            if(cl.hasOption("d")) {
                detailedView = Boolean.parseBoolean(cl.getOptionValue("d"));
            }
            if(cl.hasOption("n")) {
                skipColumnNames = Boolean.parseBoolean(cl.getOptionValue("n"));
            }
            if(cl.hasOption("S")) {
                sorted = Boolean.parseBoolean(cl.getOptionValue("S"));
            }

            execute(new ZkConfiguration());
        }
    };



    protected static Command LOG_METRICS_COMMAND = new ProtocolCommand("logMetrics",
            "Subscribes to the Metrics updates and logs them to log file or console") {

        private OutputType outputType;

        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            new MetricLogger(outputType, protocol).join();
        }

        private OutputType parseType(String typeString) {
            for (OutputType outputType : OutputType.values()) {
                if (typeString.equalsIgnoreCase(outputType.name())) {
                    return outputType;
                }
            }
            return null;
        }

        @Override
        public Options getOpts() {
            //"[sysout|log4j]",
            Options options = new Options();
            options.addOption("o", "out", true, "[sysout print to console, |log4j log4j to print]");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            String out = cl.getOptionValue("o");
            outputType = parseType(out);
            if (outputType == null) {
                throw new IllegalArgumentException("need to specify one of " + Arrays.asList(OutputType.values())
                        + " as output type");
            }

            execute(new ZkConfiguration());
        }
    };

    protected static Command START_GUI_COMMAND = new Command("startGui",
            "Starts the web based katta.gui") {

        /**
         * 端口号
         */
        private int port = 8080;


        /**
         * war文件
         */
        private File war;

        @Override
        public Options getOpts() {
            //"[-war <pathToWar>] [-port <port>]"
            Options options = new Options();
            options.addOption("w", "war", true, "a java web archive path for local.");
            options.addOption("p", "port", true, "start jetty server for port(default 8080).");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            war = new File(cl.getOptionValue("w"));
            if(cl.hasOption("p")) {
                port = Integer.parseInt(cl.getOptionValue("p"));
            }
            execute(new ZkConfiguration());
        }

        @Override
        public void execute(ZkConfiguration zkConf) throws Exception {
            List<String> paths = new ArrayList<String>();
            if (war != null) {
                paths.add(war.getAbsolutePath());
            } else {
                paths.add(".");
                paths.add("./extras/katta.gui");
            }

            WebApp app = new WebApp(paths.toArray(new String[paths.size()]), port);
            app.startWebServer();
        }
    };

    protected static Command SHOW_STRUCTURE_COMMAND = new ProtocolCommand("showStructure",
            "Shows the structure of a Katta installation. ") {

        private boolean detailedView;


        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            protocol.showStructure(detailedView);
        }

        @Override
        public Options getOpts() {
            Options options = new Options();
            options.addOption("d", true, "for detailed view.");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            if(cl.hasOption("d")) {
                detailedView = Boolean.parseBoolean(cl.getOptionValue("d"));
            }
            execute(new ZkConfiguration());
        }
    };

    protected static Command CHECK_COMMAND = new ProtocolCommand(
            "check",
            "Analyze index/shard/node status. ") {

        private boolean batchMode;
        private boolean skipColumnNames;
        private boolean sorted;

        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            System.out.println("            Index Analysis");
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            List<String> indices = protocol.getIndices();
            if (sorted) {
                Collections.sort(indices);
            }
            CounterMap<IndexState> indexStateCounterMap = new CounterMap<IndexState>();
            for (String index : indices) {
                IndexMetaData indexMD = protocol.getIndexMD(index);
                if (indexMD.hasDeployError()) {
                    indexStateCounterMap.increment(IndexState.ERROR);
                } else {
                    indexStateCounterMap.increment(IndexState.DEPLOYED);
                }
            }
            Table tableIndexStates = new Table("Index State", "Count");
            tableIndexStates.setBatchMode(batchMode);
            tableIndexStates.setSkipColumnNames(skipColumnNames);
            List<IndexState> keySet = new ArrayList<IndexState>(indexStateCounterMap.keySet());
            if (sorted) {
                Collections.sort(keySet);
            }
            for (IndexState indexState : keySet) {
                tableIndexStates.addRow(indexState, indexStateCounterMap.getCount(indexState));
            }
            System.out.println(tableIndexStates.toString());
            printResume("indices", indices.size(), indexStateCounterMap.getCount(IndexState.DEPLOYED), "deployed");

            System.out.println("\n");
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            System.out.println("            Shard Analysis");
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
            int totalShards = 0;
            for (String index : indices) {
                System.out.println("checking " + index + " ...");
                IndexMetaData indexMD = protocol.getIndexMD(index);
                ReplicationReport replicationReport = protocol.getReplicationReport(indexMD);
                Set<Shard> shards = indexMD.getShards();
                // cannot sort shards because Shard is declared inside IndexMetaData
                totalShards += shards.size() * indexMD.getReplicationLevel();
                for (Shard shard : shards) {
                    int shardReplication = replicationReport.getReplicationCount(shard.getName());
                    if (shardReplication < indexMD.getReplicationLevel()) {
                        System.out.println("\tshard " + shard + " is under-replicated (" + shardReplication + "/"
                                + indexMD.getReplicationLevel() + ")");
                    } else if (shardReplication > indexMD.getReplicationLevel()) {
                        System.out.println("\tshard " + shard + " is over-replicated (" + shardReplication + "/"
                                + indexMD.getReplicationLevel() + ")");
                    }
                }
            }

            long startTime = Long.MAX_VALUE;
            List<String> knownNodes = protocol.getKnownNodes();
            List<String> connectedNodes = protocol.getLiveNodes();
            Table tableNodeLoad = new Table("Node", "Connected", "Shard Status");
            tableNodeLoad.setBatchMode(batchMode);
            tableNodeLoad.setSkipColumnNames(skipColumnNames);
            if (sorted) {
                Collections.sort(knownNodes);
            }
            int publishedShards = 0;
            for (String node : knownNodes) {
                boolean isConnected = connectedNodes.contains(node);
                int shardCount = 0;
                int announcedShardCount = 0;
                for (String shard : protocol.getNodeShards(node)) {
                    shardCount++;
                    long ctime = protocol.getShardAnnounceTime(node, shard);
                    if (ctime > 0) {
                        announcedShardCount++;
                        if (ctime < startTime) {
                            startTime = ctime;
                        }
                    }
                }
                publishedShards += announcedShardCount;
                StringBuilder builder = new StringBuilder();
                builder.append(String.format(" %9s ", String.format("%d/%d", announcedShardCount, shardCount)));
                for (int i = 0; i < shardCount; i++) {
                    builder.append(i < announcedShardCount ? "#" : "-");
                }
                tableNodeLoad.addRow(node, Boolean.toString(isConnected), builder);
            }
            System.out.println();
            printResume("shards", totalShards, publishedShards, "deployed");
            if (startTime < Long.MAX_VALUE && totalShards > 0 && publishedShards > 0 && publishedShards < totalShards) {
                long elapsed = System.currentTimeMillis() - startTime;
                double timePerShard = (double) elapsed / (double) publishedShards;
                long remaining = Math.round(timePerShard * (totalShards - publishedShards));
                Date finished = new Date(System.currentTimeMillis() + remaining);
                remaining /= 1000;
                long secs = remaining % 60;
                remaining /= 60;
                long min = remaining % 60;
                remaining /= 60;
                System.out.printf("Estimated completion: %s (%dh %dm %ds)", finished, remaining, min, secs);
            }

            System.out.println("\n\n");
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            System.out.println("            Node Analysis");
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

            System.out.println(tableNodeLoad);
            printResume("nodes", knownNodes.size(), connectedNodes.size(), "connected");
        }

        private void printResume(String name, int maximum, int num, String action) {
            double progress = maximum == 0 ? 0.0 : (double) num / (double) maximum;
            System.out.printf("%d out of %d " + name + " " + action + " (%.2f%%)\n", num, maximum, 100 * progress);
        }

        @Override
        public Options getOpts() {
            //"[-b] [-n] [-S]",
            Options options = new Options();
            options.addOption("b", true, "for batch mode.");
            options.addOption("n", true, "don't write column names.");
            options.addOption("S", true, "for sorting the index/shard/node names.");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            if(cl.hasOption("b")) {
                batchMode = Boolean.parseBoolean(cl.getOptionValue("b"));
            }
            if(cl.hasOption("n")) {
                skipColumnNames = Boolean.parseBoolean(cl.getOptionValue("n"));
            }
            if(cl.hasOption("S")) {
                sorted = Boolean.parseBoolean(cl.getOptionValue("S"));
            }

            execute(new ZkConfiguration());
        }
    };



    protected static Command VERSION_COMMAND = new Command("version", "Print the version") {

        @Override
        public Options getOpts() {
            Options options = new Options();
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            execute(new ZkConfiguration());
        }

        @Override
        public void execute(ZkConfiguration zkConf) throws Exception {
            com.ivyft.katta.protocol.metadata.Version versionInfo = com.ivyft.katta.protocol.metadata.Version.readFromJar();
            System.out.println("Katta '" + versionInfo.getNumber() + "'");
            System.out.println("Implementation-Version '" + versionInfo.getRevision() + "'");
            System.out.println("Built by '" + versionInfo.getCompiledBy() + "' on '" + versionInfo.getCompileTime() + "'");
        }
    };


    protected static Command ADD_INDEX_COMMAND = new ProtocolCommand("addIndex",
            "Add a index to Katta") {

        private String name;
        private String collectionName;
        private String path;
        private int replicationLevel = 2;


        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            addIndex(protocol, name, collectionName, path, replicationLevel);
        }

        @Override
        public Options getOpts() {
            //"<> <collection name> <path to index> [<>]",
            Options options = new Options();
            options.addOption("i", "index", true, "index name.");
            options.addOption("c", "core", true, "Solr Core, Installed for Katta Solr(solr.xml).");
            options.addOption("p", "path", true, "index path. support hdfs|file protocol.");
            options.addOption("r", "replication", true, "replication level.");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            this.name = cl.getOptionValue("i");
            this.collectionName = cl.getOptionValue("c");
            this.path = cl.getOptionValue("p");

            if(path.startsWith("hdfs://")) {
                //hadoop 文件系统, replicationLevel = 1
                if (cl.hasOption("r")) {
                    this.replicationLevel = Integer.parseInt(cl.getOptionValue("r"));
                }

                if(this.replicationLevel != 1) {
                    this.replicationLevel = 1;
                    System.out.print("hdfs index path, replicationLevel must eq 1, use default 1");
                }
            } else {
                if (cl.hasOption("r")) {
                    this.replicationLevel = Integer.parseInt(cl.getOptionValue("r"));
                }
            }

            if(StringUtils.isBlank(name) || StringUtils.isBlank(path) || StringUtils.isBlank(collectionName)) {
                throw new IllegalArgumentException("(-i or --index) and (-p or --path) and (-c or --core) must not be null.");
            }

            execute(new ZkConfiguration());
        }
    };



    protected static Command ADD_SHARD_COMMAND = new ProtocolCommand("addShard",
            "Add a Shard to Katta index") {

        private String indexName;
        private String path;

        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            addShard(protocol, indexName, path);
        }

        @Override
        public Options getOpts() {
            Options options = new Options();
            options.addOption("i", "index", true, "index name.");
            options.addOption("p", "path", true, "index path. support hdfs|file protocol.");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            this.indexName = cl.getOptionValue("i");
            this.path = cl.getOptionValue("p");

            if(StringUtils.isBlank(indexName) || StringUtils.isBlank(path)) {
                throw new IllegalArgumentException("(-i or --index) and (-p or --path) must not be null.");
            }
            execute(new ZkConfiguration());
        }
    };

    protected static Command REMOVE_INDEX_COMMAND = new ProtocolCommand("removeIndex",
            "Remove a index from Katta") {
        private String indexName;


        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            removeIndex(protocol, indexName);
            System.out.println("undeployed index '" + indexName + "'");
        }

        @Override
        public Options getOpts() {
            //"<index name>",
            Options options = new Options();
            options.addOption("i", "index", true, "index name.");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            indexName = cl.getOptionValue("i");

            if(StringUtils.isBlank(indexName)) {
                throw new IllegalArgumentException("-i or --index must not be null.");
            }
            execute(new ZkConfiguration());
        }
    };

    protected static Command REDEPLOY_INDEX_COMMAND = new ProtocolCommand("redeployIndex",
            "Undeploys and deploys an index") {

        private String indexName;


        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            IndexMetaData indexMD = protocol.getIndexMD(indexName);
            if (indexMD == null) {
                throw new IllegalArgumentException("index '" + indexName + "' does not exist");
            }
            removeIndex(protocol, indexName);
            Thread.sleep(5000);

            addIndex(protocol, indexName, indexMD.getPath(), indexMD.getCollectionName(), indexMD.getReplicationLevel());
        }

        @Override
        public Options getOpts() {
            Options options = new Options();
            options.addOption("i", "index", true, "index name.");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            indexName = cl.getOptionValue("i");

            if(StringUtils.isBlank(indexName)) {
                throw new IllegalArgumentException("-i or --index must not be null.");
            }
            execute(new ZkConfiguration());
        }
    };

    protected static Command LIST_ERRORS_COMMAND = new ProtocolCommand("listErrors",
            "Lists all deploy errors for a specified index") {

        private String indexName;

        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            IndexMetaData indexMD = protocol.getIndexMD(indexName);
            if (indexMD == null) {
                throw new IllegalArgumentException("index '" + indexName + "' does not exist");
            }
            if (!indexMD.hasDeployError()) {
                System.out.println("No error for index '" + indexName + "'");
                return;
            }
            IndexDeployError deployError = indexMD.getDeployError();
            System.out.println("Error Type: " + deployError.getErrorType());
            if (deployError.getException() != null) {
                System.out.println("Error Message: " + deployError.getException().getMessage());
            }
            System.out.println("List of shard-errors:");
            Set<Shard> shards = indexMD.getShards();
            for (Shard shard : shards) {
                List<Exception> shardErrors = deployError.getShardErrors(shard.getName());
                if (shardErrors != null && !shardErrors.isEmpty()) {
                    System.out.println("\t" + shard.getName() + ": ");
                    for (Exception exception : shardErrors) {
                        System.out.println("\t\t" + exception.getMessage());
                    }
                }
            }
        }


        @Override
        public Options getOpts() {
            Options options = new Options();
            options.addOption("i", "index", true, "index name.");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            indexName = cl.getOptionValue("i");

            if(StringUtils.isBlank(indexName)) {
                throw new IllegalArgumentException("-i or --index must not be null.");
            }

            execute(new ZkConfiguration());
        }
    };

    protected static Command SEARCH_COMMAND = new ProtocolCommand(
            "search",
            "Search in supplied indices. The query should be in \". If you supply a result count hit details will be printed. To search in all indices write \"*\". This uses the client type LuceneClient.") {

        private String[] indexNames;
        private String query;
        private int count;

        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            if (count > 0) {
                search(indexNames, query, count);
            } else {
                search(indexNames, query);
            }
        }

        void search(final String[] indexNames, final String queryString, final int count) throws Exception {
            final ILuceneClient client = new LuceneClient();

            SolrQuery solrQuery = new SolrQuery(queryString);
            solrQuery.setRows(count);

            final long start = System.currentTimeMillis();
            final Hits hits = client.search(solrQuery, indexNames);
            final long end = System.currentTimeMillis();
            System.out.println(hits.size() + " hits found in " + ((end - start) / 1000.0) + "sec.");
            int index = 0;
            final Table table = new Table(new String[]{"Hit", "Node", "Shard", "DocId", "Score"});
            for (final Hit hit : hits.getHits()) {
                table.addRow(index, hit.getNode(), hit.getShard(), hit.getDocId(), hit.getScore());
                index++;
            }
            System.out.println(table.toString());
            client.close();
        }

        void search(final String[] indexNames, final String queryString) throws Exception {
            final ILuceneClient client = new LuceneClient();

            SolrQuery solrQuery = new SolrQuery(queryString);
            solrQuery.setRows(100);

            final long start = System.currentTimeMillis();
            final int hitsSize = client.count(solrQuery, indexNames);
            final long end = System.currentTimeMillis();
            System.out.println(hitsSize + " Hits found in " + ((end - start) / 1000.0) + "sec.");
            client.close();
        }

        @Override
        public Options getOpts() {
            //"<index name>[,<index name>,...] \"<query>\" [count]"
            Options options = new Options();
            options.addOption("i", "index", true, "query indices for register.");
            options.addOption("q", "query", true, "solr query");
            options.addOption("c", "count", true, "returns count");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            indexNames = cl.getOptionValues("i");
            query = cl.getOptionValue("q");
            if (cl.hasOption("c")) {
                count = Integer.parseInt(cl.getOptionValue("c"));
            }

            if(indexNames == null || indexNames.length == 0 || StringUtils.isBlank(query)) {
                throw new IllegalArgumentException("-i or --index and q or query must not be null.");
            }

            execute(new ZkConfiguration());
        }
    };




    protected static Command GENERATE_INDEX_COMMAND = new Command(
            "generateIndex",
            "The inputTextFile is used as dictionary. The field name is 'text', so search with queries like 'text:aWord' in the index.") {

        private String input;
        private String output;
        private int wordsPerDoc;
        private int indexSize;


        @Override
        public Options getOpts() {
            Options options = new Options();
            options.addOption("i", "input", true, "inputTextFile");
            options.addOption("o", "output", true, "outputPath");
            options.addOption("w", "words", true, "numOfWordsPerDoc");
            options.addOption("d", "docs", true, "numOfDocuments");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            input = cl.getOptionValue("i");
            output = cl.getOptionValue("o");
            wordsPerDoc = Integer.parseInt(cl.getOptionValue("w"));
            indexSize = Integer.parseInt(cl.getOptionValue("d"));

            execute(new ZkConfiguration());
        }

        @Override
        public void execute(ZkConfiguration zkConf) throws Exception {
            SampleIndexGenerator sampleIndexGenerator = new SampleIndexGenerator();
            sampleIndexGenerator.createIndex(input, output, wordsPerDoc, indexSize);
        }

    };

    protected static Command RUN_CLASS_COMMAND = new Command("runclass",
            "runs a custom class") {

        private Class<?> clazz;
        private Method   method;
        private String[] methodArgs;

        @Override
        public Options getOpts() {
            Options options = new Options();
            options.addOption("c", "class", true, "");
            options.addOption("m", "method", true, "");
            options.addOption("a", "args", true, "");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            clazz = ClassUtil.forName(cl.getOptionValue("c"), Object.class);
            methodArgs = cl.getOptionValues("m");
            method = clazz.getMethod("main", methodArgs.getClass());
            if (method == null) {
                throw new IllegalArgumentException("class " + clazz.getName() + " doesn't have a main method");
            }

            execute(new ZkConfiguration());
        }

        @Override
        public void execute(ZkConfiguration zkConf) throws Exception {
            method.invoke(null, new Object[]{ methodArgs });
        }

    };

    public static void main(String[] args) throws Exception {
        commands.put(help.getCommand(), help);
        commands.put(START_ZK_COMMAND.getCommand(), START_ZK_COMMAND);
        commands.put(START_MASTER_COMMAND.getCommand(), START_MASTER_COMMAND);

        KattaNode kattaNode = new KattaNode();
        commands.put(kattaNode.getCommand(), kattaNode);


        SingleKattaServer kattaServer = new SingleKattaServer();
        commands.put(kattaServer.getCommand(), kattaServer);


        commands.put(LIST_NODES_COMMAND.getCommand(), LIST_NODES_COMMAND);
        commands.put(LIST_INDICES_COMMAND.getCommand(), LIST_INDICES_COMMAND);
        commands.put(LOG_METRICS_COMMAND.getCommand(), LOG_METRICS_COMMAND);
        commands.put(START_GUI_COMMAND.getCommand(), START_GUI_COMMAND);
        commands.put(SHOW_STRUCTURE_COMMAND.getCommand(), SHOW_STRUCTURE_COMMAND);
        commands.put(CHECK_COMMAND.getCommand(), CHECK_COMMAND);
        commands.put(VERSION_COMMAND.getCommand(), VERSION_COMMAND);

        CreateIndex createIndex = new CreateIndex();
        commands.put(createIndex.getCommand(), createIndex);


        commands.put(ADD_INDEX_COMMAND.getCommand(), ADD_INDEX_COMMAND);
        commands.put(ADD_SHARD_COMMAND.getCommand(), ADD_SHARD_COMMAND);
        commands.put(REMOVE_INDEX_COMMAND.getCommand(), REMOVE_INDEX_COMMAND);
        commands.put(REDEPLOY_INDEX_COMMAND.getCommand(), REDEPLOY_INDEX_COMMAND);
        commands.put(LIST_ERRORS_COMMAND.getCommand(), LIST_ERRORS_COMMAND);
        commands.put(SEARCH_COMMAND.getCommand(), SEARCH_COMMAND);
        commands.put(GENERATE_INDEX_COMMAND.getCommand(), GENERATE_INDEX_COMMAND);
        commands.put(RUN_CLASS_COMMAND.getCommand(), RUN_CLASS_COMMAND);

        KattaOnYarn kattaOnYarn = new KattaOnYarn();
        commands.put(kattaOnYarn.getCommand(), kattaOnYarn);

        YarnStartNodes yarnStartNodes = new YarnStartNodes();
        commands.put(yarnStartNodes.getCommand(), yarnStartNodes);

        YarnListNodes yarnListNodes = new YarnListNodes();
        commands.put(yarnListNodes.getCommand(), yarnListNodes);

        YarnStopNodes yarnStopNodes = new YarnStopNodes();
        commands.put(yarnStopNodes.getCommand(), yarnStopNodes);

        String commandName = null;
        String[] commandArgs = null;
        if (args.length < 1) {
            commandName = "help";
            commandArgs = new String[0];
        } else {
            commandName = args[0];
            commandArgs = Arrays.copyOfRange(args, 1, args.length);
        }

        Command command = commands.get(commandName);
        if(command == null) {
            log.error("ERROR: " + commandName + " is not a supported command.");
            printHelpFor(null);
            System.exit(1);
        }
        Options opts = command.getOpts();
        if(!opts.hasOption("h")) {
            opts.addOption("h", "help", false, "print out a help message");
        }
        CommandLine cl = new GnuParser().parse(command.getOpts(), commandArgs);
        boolean showStackTrace = cl.hasOption("s");

        if(cl.hasOption("help")) {
            printHelpFor(Arrays.asList(commandName));
        } else {
            try {
                command.process(cl);
            } catch (Exception e) {
                printError(e.getMessage());
                if (showStackTrace) {
                    e.printStackTrace();
                }
                HelpFormatter f = new HelpFormatter();
                f.printHelp(command.getCommand(),
                        command.getHeaderDescription(),
                        command.getOpts(),
                        showStackTrace ? null : printUsageFooter());
                System.exit(1);
            }
        }
    }


    public static void printHelpFor(Collection<String> args) {
        if(args == null || args.size() < 1) {
            args = commands.keySet();
        }
        HelpFormatter f = new HelpFormatter();
        for(String command: args) {
            Command c = commands.get(command);
            if (c != null) {
                f.printHelp(command, c.getHeaderDescription(), c.getOpts(), null);
                System.out.println();
            } else {
                System.err.println("ERROR: " + c + " is not a supported command.");
            }
        }
    }

    private static String printUsageFooter() {
        return "\nGlobal Options:" +
                "  -s\t\tshow stacktrace\n";
    }

    private static void printUsageHeader() {
        System.err.println("Usage: ");
    }




    /**
     * 手动的添加一份shard
     * @param protocol zk包装的协议
     * @param name shardName
     * @param path shardIndex索引地址
     * @param replicationLevel 复制次数,份数
     */
    protected static void addIndex(InteractionProtocol protocol, String name, String collectionName, String path, int replicationLevel) {
        if (name.trim().equals("*")) {
            throw new IllegalArgumentException("Index with name " + name + " isn't allowed.");
        }

        //用于判断当前是否已经部署过这个shardName的shard
        IDeployClient deployClient = new DeployClient(protocol);
        if (deployClient.existsIndex(name)) {
            throw new IllegalArgumentException("Index with name " + name + " already exists.");
        }

        try {
            long startTime = System.currentTimeMillis();
            IIndexDeployFuture deployFuture = deployClient.addIndex(name, path, collectionName, replicationLevel);
            while (true) {
                long duration = System.currentTimeMillis() - startTime;
                if (deployFuture.getState() == IndexState.DEPLOYED) {
                    System.out.println("\ndeployed index '" + name + "' in " + duration + " ms");
                    break;
                } else if (deployFuture.getState() == IndexState.ERROR) {
                    System.err.println("\nfailed to deploy index '" + name + "' in " + duration + " ms");
                    break;
                }
                System.out.print(".");
                deployFuture.joinDeployment(1000);
            }
        } catch (final InterruptedException e) {
            printError("interrupted wait on index deployment");
        }
    }

    protected static void removeIndex(InteractionProtocol protocol, final String indexName) {
        IDeployClient deployClient = new DeployClient(protocol);
        if (!deployClient.existsIndex(indexName)) {
            throw new IllegalArgumentException("index '" + indexName + "' does not exist");
        }
        deployClient.removeIndex(indexName);
    }


    /**
     * 手动的添加一份shard到一个Index
     * @param protocol zk包装的协议
     * @param name shardName
     * @param path shardIndex索引地址
     */
    protected static void addShard(InteractionProtocol protocol,
                                   String name, String path) {
        //用于判断当前是否已经部署过这个shardName的shard
        IDeployClient deployClient = new DeployClient(protocol);
        if (!deployClient.existsIndex(name)) {
            throw new IllegalArgumentException("Index with name " + name + " not exists yet.");
        }

        try {
            long startTime = System.currentTimeMillis();
            IIndexDeployFuture deployFuture = deployClient.addShard(name, path);
            while (true) {
                long duration = System.currentTimeMillis() - startTime;
                if (deployFuture.getState() == IndexState.DEPLOYED) {
                    System.out.println("\ndeployed shard '" + path + "' to "
                            + name + " in " + duration + " ms");
                    break;
                } else if (deployFuture.getState() == IndexState.ERROR) {
                    System.err.println("\nfailed to deploy shard '" + path + "' to "
                            + name + " in " + duration + " ms");
                    break;
                }
                System.out.print(".");
                deployFuture.joinDeployment(1000);
            }
        } catch (final InterruptedException e) {
            printError("interrupted wait shard on index deployment");
        }
    }


    protected static void validateMinArguments(String[] args, int minCount) {
        if (args.length < minCount) {
            throw new IllegalArgumentException("not enough arguments");
        }
    }

    public static void printError(String errorMsg) {
        System.err.println("ERROR: " + errorMsg);
    }

}
