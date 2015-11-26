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
import com.ivyft.katta.client.loadtest.LoadTestMasterOperation;
import com.ivyft.katta.client.mapfile.AbstractQueryExecutor;
import com.ivyft.katta.client.mapfile.MapfileAccessExecutor;
import com.ivyft.katta.client.query.LuceneSearchExecutor;
import com.ivyft.katta.lib.lucene.*;
import com.ivyft.katta.master.Master;
import com.ivyft.katta.node.IContentServer;
import com.ivyft.katta.node.Node;
import com.ivyft.katta.node.monitor.MetricLogger;
import com.ivyft.katta.node.monitor.MetricLogger.OutputType;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.ReplicationReport;
import com.ivyft.katta.protocol.metadata.IndexDeployError;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.protocol.metadata.NodeMetaData;
import com.ivyft.katta.protocol.metadata.Shard;
import com.ivyft.katta.tool.SampleIndexGenerator;
import com.ivyft.katta.util.*;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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

    protected static final Logger log = LoggerFactory.getLogger(Katta.class);
    private final static List<Command> COMMANDS = new ArrayList<Command>();

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            printUsageAndExit();
        }
        boolean showStackTrace = parseOptionMap(args).containsKey("-s");
        if (showStackTrace) {
            args = removeArg(args, "-s");
        }
        Command command = null;
        try {
            command = getCommand(args[0]);
            command.parseArguments(args);
            command.execute();
        } catch (Exception e) {
            printError(e.getMessage());
            if (showStackTrace) {
                e.printStackTrace();
            }
            if (command != null) {
                printUsageHeader();
                printUsage(command);
                printUsageFooter();
            }
            // e.printStackTrace();
            System.exit(1);
        }
    }

    private static String[] removeArg(String[] args, String argToRemove) {
        List<String> newArgs = new ArrayList<String>();
        for (String arg : args) {
            if (!arg.equals(argToRemove)) {
                newArgs.add(arg);
            }
        }
        return newArgs.toArray(new String[newArgs.size()]);
    }

    protected static Command getCommand(String commandString) {
        for (Command command : COMMANDS) {
            if (commandString.equalsIgnoreCase(command.getCommand())) {
                return command;
            }
        }
        throw new IllegalArgumentException("no command for '" + commandString + "' found");
    }

    private static void printUsage(Command command) {
        System.err.println("  "
                + StringUtil.fillWithWhiteSpace(command.getCommand() + " " + command.getParameterString(), 60) + " "
                + command.getDescription());
    }

    private static void printUsageAndExit() {
        printUsageHeader();
        for (Command command : COMMANDS) {
            printUsage(command);
        }
        printUsageFooter();
        System.exit(1);
    }

    private static void printUsageFooter() {
        System.err.println();
        System.err.println("Global Options:");
        System.err.println("  -s\t\tshow stacktrace");
        System.err.println();
    }

    private static void printUsageHeader() {
        System.err.println("Usage: ");
    }

    protected static Map<String, String> parseOptionMap(final String[] args) {
        Map<String, String> optionMap = new HashMap<String, String>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-")) {
                String value = null;
                if (i < args.length - 1 && !args[i + 1].startsWith("-")) {
                    value = args[i + 1];
                }
                optionMap.put(args[i], value);
            }
        }
        return optionMap;
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

    private static void printError(String errorMsg) {
        System.err.println("ERROR: " + errorMsg);
    }

    protected static Command START_ZK_COMMAND = new Command("startZk", "", "Starts a local zookeeper server") {

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
                System.out.println("zookeeper server started on port " + zkServer.getPort());
                zkServer.wait();
            }
        }
    };

    protected static Command START_MASTER_COMMAND = new Command("master", "[-e] [-ne]",
            "Starts a local master. -e & -ne for embedded and non-embedded zk-server (overriding configuration)") {

        private boolean embeddedMode;

        protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap)
                throws Exception {
            if (optionMap.containsKey("-e")) {
                embeddedMode = true;
            } else if (optionMap.containsKey("-ne")) {
                embeddedMode = false;
            } else {
                embeddedMode = zkConf.isEmbedded();
            }
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

    protected static Command START_NODE_COMMAND = new ProtocolCommand("node",
            "[-c <serverClass>] [-p <port number>]", "Starts a local node") {

        private NodeConfiguration nodeConfiguration;
        private IContentServer server = null;

        @Override
        protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
            nodeConfiguration = new NodeConfiguration();
            String serverClassName;
            if (optionMap.containsKey("-c")) {
                serverClassName = optionMap.get("-c");
            } else {
                serverClassName = nodeConfiguration.getServerClassName();
            }
            if (optionMap.containsKey("-p")) {
                String portNumber = optionMap.get("-p");
                nodeConfiguration.setStartPort(Integer.parseInt(portNumber));
            }

            Class<?> serverClass = ClassUtil.forName(serverClassName, IContentServer.class);
            server = (IContentServer) ClassUtil.newInstance(serverClass);
        }

        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            SolrHandler.init(nodeConfiguration.getFile("node.solrhome.folder"));
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

    };

    protected static Command LIST_NODES_COMMAND = new ProtocolCommand("listNodes", "[-d] [-b] [-n] [-S]",
            "Lists all nodes. -b for batch mode, -n don't write column headers, -S for sorting the node names.") {

        private boolean batchMode;
        private boolean skipColumnNames;
        private boolean sorted;

        @Override
        protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
            batchMode = optionMap.containsKey("-b");
            skipColumnNames = optionMap.containsKey("-n");
            sorted = optionMap.containsKey("-S");
        }

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
    };

    protected static Command LIST_INDICES_COMMAND = new ProtocolCommand(
            "listIndices",
            "[-d] [-b] [-n] [-S]",
            "Lists all indices. -d for detailed view, -b for batch mode, -n don't write column headers, -S for sorting the shard names.") {

        private boolean detailedView;
        private boolean batchMode;
        private boolean skipColumnNames;
        private boolean sorted;

        @Override
        protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
            detailedView = optionMap.containsKey("-d");
            batchMode = optionMap.containsKey("-b");
            skipColumnNames = optionMap.containsKey("-n");
            sorted = optionMap.containsKey("-S");
        }

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
    };

    protected static Command LOG_METRICS_COMMAND = new ProtocolCommand("logMetrics", "[sysout|log4j]",
            "Subscribes to the Metrics updates and logs them to log file or console") {

        private OutputType outputType;

        @Override
        protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
            if (args.length < 2) {
                throw new IllegalArgumentException("no output type specified");
            }
            if (parseType(args[1]) == null) {
                throw new IllegalArgumentException("need to specify one of " + Arrays.asList(OutputType.values())
                        + " as output type");
            }
            outputType = parseType(args[1]);

        }

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

    };

    protected static Command START_GUI_COMMAND = new Command("startGui", "[-war <pathToWar>] [-port <port>]",
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
        protected void parseArguments(ZkConfiguration zkConf,
                                      String[] args,
                                      Map<String, String> optionMap) {
            if (optionMap.containsKey("-war")) {
                war = new File(optionMap.get("-war"));
            }
            if (optionMap.containsKey("-port")) {
                port = Integer.parseInt(optionMap.get("-port"));
            }
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

    protected static Command SHOW_STRUCTURE_COMMAND = new ProtocolCommand("showStructure", "[-d]",
            "Shows the structure of a Katta installation. -d for detailed view.") {

        private boolean _detailedView;

        @Override
        protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
            _detailedView = optionMap.containsKey("-d");
        }

        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            protocol.showStructure(_detailedView);
        }
    };

    protected static Command CHECK_COMMAND = new ProtocolCommand(
            "check",
            "[-b] [-n] [-S]",
            "Analyze index/shard/node status. -b for batch mode, -n don't write column names, -S for sorting the index/shard/node names.") {

        private boolean _batchMode;
        private boolean _skipColumnNames;
        private boolean _sorted;

        @Override
        protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
            _batchMode = optionMap.containsKey("-b");
            _skipColumnNames = optionMap.containsKey("-n");
            _sorted = optionMap.containsKey("-S");
        }

        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            System.out.println("            Index Analysis");
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            List<String> indices = protocol.getIndices();
            if (_sorted) {
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
            tableIndexStates.setBatchMode(_batchMode);
            tableIndexStates.setSkipColumnNames(_skipColumnNames);
            List<IndexState> keySet = new ArrayList<IndexState>(indexStateCounterMap.keySet());
            if (_sorted) {
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
            tableNodeLoad.setBatchMode(_batchMode);
            tableNodeLoad.setSkipColumnNames(_skipColumnNames);
            if (_sorted) {
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

    };

    // System.err.println("\tversion\t\t\tPrint the version.");

    protected static Command VERSION_COMMAND = new Command("version", "", "Print the version") {

        @Override
        public void execute(ZkConfiguration zkConf) throws Exception {
            com.ivyft.katta.protocol.metadata.Version versionInfo = com.ivyft.katta.protocol.metadata.Version.readFromJar();
            System.out.println("Katta '" + versionInfo.getNumber() + "'");
            System.out.println("Implementation-Version '" + versionInfo.getRevision() + "'");
            System.out.println("Built by '" + versionInfo.getCompiledBy() + "' on '" + versionInfo.getCompileTime() + "'");
        }
    };

    protected static Command ADD_INDEX_COMMAND = new ProtocolCommand("addIndex",
            "<index name> <collection name> <path to index> [<replication level>]", "Add a index to Katta") {

        private String name;
        private String collectionName;
        private String path;
        private int replicationLevel = 3;

        @Override
        protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
            validateMinArguments(args, 4);
            this.name = args[1];
            this.collectionName = args[2];
            this.path = args[3];
            if (args.length >= 5) {
                this.replicationLevel = Integer.parseInt(args[4]);
            }
        }

        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            addIndex(protocol, name, collectionName, path, replicationLevel);
        }

    };



    protected static Command ADD_SHARD_COMMAND = new ProtocolCommand("addShard",
            "<index name> <path to index>",
            "Add a Shard to Katta index") {

        private String shardName;
        private String path;

        @Override
        protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
            validateMinArguments(args, 3);
            this.shardName = args[1];
            this.path = args[2];
        }

        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            addShard(protocol, shardName, path);
        }

    };

    protected static Command REMOVE_INDEX_COMMAND = new ProtocolCommand("removeIndex", "<index name>",
            "Remove a index from Katta") {
        private String _indexName;

        @Override
        protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
            validateMinArguments(args, 2);
            _indexName = args[1];
        }

        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            removeIndex(protocol, _indexName);
            System.out.println("undeployed index '" + _indexName + "'");
        }

    };

    protected static Command REDEPLOY_INDEX_COMMAND = new ProtocolCommand("redeployIndex", "<index name>",
            "Undeploys and deploys an index") {

        private String indexName;

        @Override
        protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
            validateMinArguments(args, 2);
            indexName = args[1];
        }

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
    };

    protected static Command LIST_ERRORS_COMMAND = new ProtocolCommand("listErrors", "<index name>",
            "Lists all deploy errors for a specified index") {

        private String _indexName;

        @Override
        protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
            validateMinArguments(args, 2);
            _indexName = args[1];
        }

        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            IndexMetaData indexMD = protocol.getIndexMD(_indexName);
            if (indexMD == null) {
                throw new IllegalArgumentException("index '" + _indexName + "' does not exist");
            }
            if (!indexMD.hasDeployError()) {
                System.out.println("No error for index '" + _indexName + "'");
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

    };

    protected static Command SEARCH_COMMAND = new ProtocolCommand(
            "search",
            "<index name>[,<index name>,...] \"<query>\" [count]",
            "Search in supplied indices. The query should be in \". If you supply a result count hit details will be printed. To search in all indices write \"*\". This uses the client type LuceneClient.") {

        private String[] _indexNames;
        private String _query;
        private int _count;

        @Override
        protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
            validateMinArguments(args, 3);
            _indexNames = args[1].split(",");
            _query = args[2];
            if (args.length > 3) {
                try {
                    _count = Integer.parseInt(args[3]);
                } catch (Exception e) {

                }
            }
        }

        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            if (_count > 0) {
                search(_indexNames, _query, _count);
            } else {
                search(_indexNames, _query);
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

    };

    protected static Command GENERATE_INDEX_COMMAND = new Command(
            "generateIndex",
            "<inputTextFile> <outputPath>  <numOfWordsPerDoc> <numOfDocuments>",
            "The inputTextFile is used as dictionary. The field name is 'text', so search with queries like 'text:aWord' in the index.") {

        private String _input;
        private String _output;
        private int _wordsPerDoc;
        private int _indexSize;

        @Override
        protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
            validateMinArguments(args, 4);
            _input = args[1];
            _output = args[2];
            _wordsPerDoc = Integer.parseInt(args[3]);
            _indexSize = Integer.parseInt(args[4]);
        }

        @Override
        public void execute(ZkConfiguration zkConf) throws Exception {
            SampleIndexGenerator sampleIndexGenerator = new SampleIndexGenerator();
            sampleIndexGenerator.createIndex(_input, _output, _wordsPerDoc, _indexSize);

        }

    };

    protected static Command LOADTEST_COMMAND = new ProtocolCommand(
            "loadtest",
            "<zkRootPath> <nodeCount> <startQueryRate> <endQueryRate> <rateStep> <durationPerIteration> <indexName> <queryFile> <resultFolder> <typeWithParameters> ",
            "Starts a load test on a katta cluster with the given zkRootPath. The query rate is in queries per second. The durationPerIteration is in milliseconds. The resultFolder will be created on the master host. typeWithParameters is one of 'lucene <maxHits>' | 'mapfile'") {

        private LoadTestMasterOperation _masterOperation;
        private File _resultFolder;

        @Override
        protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
            validateMinArguments(args, 11);
            String zkRootPath = args[1];
            int nodeCount = Integer.parseInt(args[2]);
            int startQueryRate = Integer.parseInt(args[3]);
            int endQueryRate = Integer.parseInt(args[4]);
            int rateStep = Integer.parseInt(args[5]);
            long durationPerIteration = Integer.parseInt(args[6]);
            String indexName = args[7];
            File queryFile = new File(args[8]);
            _resultFolder = new File(args[9]);
            String type = args[10];

            if (!queryFile.exists()) {
                throw new IllegalStateException("query file '" + queryFile.getAbsolutePath() + "' does not exists");
            }
            AbstractQueryExecutor queryExecutor;
            String[] indices = new String[]{indexName};
            String[] queries = readQueries(queryFile);
            if (type.equalsIgnoreCase("lucene")) {
                int maxHits = Integer.parseInt(args[11]);
                ZkConfiguration searchClusterZkConf = new ZkConfiguration();
                searchClusterZkConf.setZKServers(zkConf.getZKServers());
                searchClusterZkConf.setZKRootPath(zkRootPath);
                queryExecutor = new LuceneSearchExecutor(indices, queries, searchClusterZkConf, maxHits);
            } else if (type.equalsIgnoreCase("mapfile")) {
                queryExecutor = new MapfileAccessExecutor(indices, queries, zkConf);
            } else {
                throw new IllegalStateException("type '" + type + "' unknown");
            }

            _masterOperation = new LoadTestMasterOperation(nodeCount, startQueryRate, endQueryRate, rateStep,
                    durationPerIteration, queryExecutor, _resultFolder);
        }

        @Override
        public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
            _masterOperation.registerCompletion(protocol);
            protocol.addMasterOperation(_masterOperation);
            long startTime = System.currentTimeMillis();
            System.out.println("load test triggered - waiting on completion...");
            _masterOperation.joinCompletion(protocol);
            System.out.println("load test complete - took " + (System.currentTimeMillis() - startTime) + " ms");
            System.out.println("find the results in '" + _resultFolder.getAbsolutePath()
                    + "' on the master or inspect the master logs if the results are missing");
        }

        private String[] readQueries(File queryFile) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(queryFile));
                List<String> lines = new ArrayList<String>();
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (!line.equals("")) {
                        lines.add(line);
                    }
                }
                return lines.toArray(new String[lines.size()]);
            } catch (IOException e) {
                throw new RuntimeException("Failed to read query file " + queryFile + ".", e);
            }
        }

    };

    protected static Command RUN_CLASS_COMMAND = new Command("runclass", "<className>", "runs a custom class") {

        private Class<?> _clazz;
        private Method _method;
        private String[] _methodArgs;

        @Override
        protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap)
                throws Exception {
            validateMinArguments(args, 2);
            _clazz = ClassUtil.forName(args[1], Object.class);
            _method = _clazz.getMethod("main", args.getClass());
            if (_method == null) {
                throw new IllegalArgumentException("class " + _clazz.getName() + " doesn't have a main method");
            }
            _methodArgs = Arrays.copyOfRange(args, 2, args.length);
        }

        @Override
        public void execute(ZkConfiguration zkConf) throws Exception {
            _method.invoke(null, new Object[]{_methodArgs});
        }

    };

    static {
        COMMANDS.add(START_ZK_COMMAND);
        COMMANDS.add(START_MASTER_COMMAND);
        COMMANDS.add(START_NODE_COMMAND);
        COMMANDS.add(LIST_INDICES_COMMAND);
        COMMANDS.add(LIST_NODES_COMMAND);
        COMMANDS.add(LIST_ERRORS_COMMAND);
        COMMANDS.add(ADD_INDEX_COMMAND);
        COMMANDS.add(REMOVE_INDEX_COMMAND);
        COMMANDS.add(REDEPLOY_INDEX_COMMAND);
        COMMANDS.add(CHECK_COMMAND);
        COMMANDS.add(VERSION_COMMAND);
        COMMANDS.add(SHOW_STRUCTURE_COMMAND);
        COMMANDS.add(START_GUI_COMMAND);
        COMMANDS.add(LOG_METRICS_COMMAND);
        COMMANDS.add(GENERATE_INDEX_COMMAND);
        COMMANDS.add(SEARCH_COMMAND);
        COMMANDS.add(LOADTEST_COMMAND);
        COMMANDS.add(RUN_CLASS_COMMAND);

        Set<String> commandStrings = new HashSet<String>();
        for (Command command : COMMANDS) {
            if (!commandStrings.add(command.getCommand())) {
                throw new IllegalStateException("duplicated command sting " + command.getCommand());
            }
        }
    }

    static abstract class Command {

        private final String _command;
        private final String _parameterString;
        private final String _description;

        public Command(String command, String parameterString, String description) {
            _command = command;
            _parameterString = parameterString;
            _description = description;
        }

        public final void parseArguments(String[] args) throws Exception {
            parseArguments(new ZkConfiguration(), args, parseOptionMap(args));
        }

        public final void parseArguments(ZkConfiguration zkConf, String[] args) throws Exception {
            parseArguments(zkConf, args, parseOptionMap(args));
        }

        protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap)
                throws Exception {
            // subclasses may override
        }

        public void execute() throws Exception {
            execute(new ZkConfiguration());
        }

        protected abstract void execute(ZkConfiguration zkConf) throws Exception;

        public String getCommand() {
            return _command;
        }

        public String getParameterString() {
            return _parameterString;
        }

        public String getDescription() {
            return _description;
        }
    }

    static abstract class ProtocolCommand extends Command {

        public ProtocolCommand(String command, String parameterString, String description) {
            super(command, parameterString, description);
        }

        @Override
        public final void execute(ZkConfiguration zkConf) throws Exception {
            ZkClient zkClient = new ZkClient(zkConf.getZKServers());
            InteractionProtocol protocol = new InteractionProtocol(zkClient, zkConf);
            execute(zkConf, protocol);
            protocol.disconnect();
        }

        protected abstract void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception;
    }
}
