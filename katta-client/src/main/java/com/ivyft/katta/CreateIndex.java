package com.ivyft.katta;

import com.ivyft.katta.client.DeployClient;
import com.ivyft.katta.client.IDeployClient;
import com.ivyft.katta.client.IIndexDeployFuture;
import com.ivyft.katta.client.IndexState;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.NewIndexMetaData;
import com.ivyft.katta.util.HadoopUtil;
import com.ivyft.katta.util.MasterConfiguration;
import com.ivyft.katta.util.ZkConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/11
 * Time: 13:51
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class CreateIndex extends ProtocolCommand {
    public CreateIndex() {
        super("createIndex",
                "created a index to Katta");
    }


    private String name;
    private String path;
    private int shardNum;
    private int shardStep;



    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
        createIndex(protocol, name, path, shardNum, shardStep);
    }

    @Override
    public Options getOpts() {
        //"<> <collection name> <path to index> [<>]",
        Options options = new Options();
        options.addOption("i", "index", true, "index name.");
        options.addOption("n", "shardNum", true, "Shard Number.");
        options.addOption("t", "shardStep", true, "shard Step Number.");
        options.addOption("p", "path", true, "index path prefix.");
        options.addOption("s", false, "print exception");
        return options;
    }

    @Override
    public void process(CommandLine cl) throws Exception {
        this.name = cl.getOptionValue("i");
        this.shardNum = Integer.parseInt(cl.getOptionValue("n"));
        this.shardStep = Integer.parseInt(cl.getOptionValue("t"));
        if (cl.hasOption("p")) {
            this.path = cl.getOptionValue("p");
        } else {
            MasterConfiguration masterConf = new MasterConfiguration();
            this.path = masterConf.getString("katta.data.storage.path");
        }

        if(StringUtils.isBlank(name) || shardNum < 2 || shardStep < 1) {
            throw new IllegalArgumentException("(-i or --index) and (-n or --shardNum) and (-t or --shardStep) must not be null.");
        }

        execute(new ZkConfiguration());
    }



    /**
     * 手动的添加一份shard
     * @param protocol zk包装的协议
     * @param name shardName
     * @param path shardIndex索引地址
     */
    protected static void createIndex(InteractionProtocol protocol,
                                      String name,
                                      String path,
                                      int shardNum,
                                      int shardStep) {
        if (name.trim().equals("*")) {
            throw new IllegalArgumentException("Index with name " + name + " isn't allowed.");
        }

        NewIndexMetaData newIndex = protocol.getNewIndex(name);
        if(newIndex != null) {
            throw new IllegalArgumentException("Index: " + name + " was exists zookeeper.");
        }

        try {
            FileSystem fs = HadoopUtil.getFileSystem();
            Path f = new Path(path, name);
            if(fs.exists(f)) {
                throw new IllegalArgumentException("Index: " + name + " was exists FileSystem. path: " + f);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        //用于判断当前是否已经部署过这个shardName的shard
        IDeployClient deployClient = new DeployClient(protocol);

        if (deployClient.existsNewIndex(name)) {
            throw new IllegalArgumentException("New Index with name " + name + " already exists.");
        }

        try {
            long startTime = System.currentTimeMillis();
            IIndexDeployFuture deployFuture = deployClient.createIndex(name, path, shardNum, shardStep);
            while (true) {
                long duration = System.currentTimeMillis() - startTime;
                if (deployFuture.getState() == IndexState.DEPLOYED) {
                    System.out.println("\ncreated index '" + name + "' in " + duration + " ms");
                    break;
                } else if (deployFuture.getState() == IndexState.ERROR) {
                    System.err.println("\nfailed to created index '" + name + "' in " + duration + " ms");
                    break;
                }
                System.out.print(".");
                deployFuture.joinDeployment(1000);
            }
        } catch (final InterruptedException e) {
            Katta.printError("interrupted wait on index deployment");
        }
    }

}
