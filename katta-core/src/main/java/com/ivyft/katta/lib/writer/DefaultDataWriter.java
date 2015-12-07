package com.ivyft.katta.lib.writer;

import com.ivyft.katta.protocol.*;
import com.ivyft.katta.util.*;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.*;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/29
 * Time: 11:55
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class DefaultDataWriter extends DataWriter {


    private int shardNum;


    private int shardStep;


    private int shardPartitions;


    protected Path indexDataStoragePath;


    protected String indexName = "unknown-index";



    protected String filePrefix = "";



    protected Map<ShardRange, Tuple<ShardRange, SerializationWriter>>
            SHARD_RANGE_MAP = new HashMap<ShardRange, Tuple<ShardRange, SerializationWriter>>();


    protected static Logger LOG = LoggerFactory.getLogger(DefaultDataWriter.class);


    public DefaultDataWriter() {

    }

    @Override
    public void init(MasterConfiguration conf, InteractionProtocol protocol, String indexId) {
        this.indexName = indexId;
        this.filePrefix = conf.getString("katta.master.code", "");
        String indexShardNum = "katta.index." + indexId + ".shard.num";
        String indexShardStep = "katta.index." + indexId + ".shard.step";
        String indexShardPartition = "katta.index." + indexId + ".numPartitions";


        String dataStoragePath = conf.getString("katta.data.storage.path");
        //String dataStoragePath = "/user/katta/data";

        final FileSystem fileSystem;
        try {
            this.indexDataStoragePath = new Path(dataStoragePath + "/" + indexId);
            //fileSystem = HadoopUtil.getFileSystem(new URI("file:///Users"));
            fileSystem = HadoopUtil.getFileSystem(indexDataStoragePath);
            fileSystem.mkdirs(indexDataStoragePath);
            LOG.info(indexId + " index data storage path: " + indexDataStoragePath);

            Path indexMetaPath = new Path(this.indexDataStoragePath, indexId + ".meta.properties");

            //旧索引
            Properties metaProp = new Properties();
            FSDataInputStream inputStream = fileSystem.open(indexMetaPath);
            try {
                metaProp.load(inputStream);
            } finally {
                inputStream.close();
            }

            this.shardNum = Integer.parseInt(metaProp.getProperty(indexShardNum));
            this.shardStep = Integer.parseInt(metaProp.getProperty(indexShardStep));

            this.shardPartitions = Integer.parseInt(metaProp.getProperty(indexShardPartition));

            if(shardNum < 2) {
                throw new IllegalArgumentException("shardNum 必须大于1. ");
            }

            if(shardStep < 1) {
                throw new IllegalArgumentException("shardStep 必须大于或等于1. ");
            }

            FileStatus[] fileStatuses = fileSystem.listStatus(indexMetaPath.getParent(), new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    try {
                        return fileSystem.isDirectory(path);
                    } catch (IOException e) {
                        return false;
                    }
                }
            });

            if(fileStatuses != null) {
                for (FileStatus statuse : fileStatuses) {
                    ShardRange range = new ShardRange(statuse.getPath().getName(), statuse.getPath().toString());
                    //读取 meta 信息

                    Properties shardProp = new Properties();

                    String shardMetaFileName = indexId + ".shard." + statuse.getPath().getName() + ".meta.properties";
                    Path shardPropPath = new Path(statuse.getPath(), "data/" + shardMetaFileName);

                    FSDataInputStream open = fileSystem.open(shardPropPath);
                    try {
                        shardProp.load(open);
                    } finally {
                        open.close();
                    }

                    range.setStart(Integer.parseInt(shardProp.getProperty("start")));
                    range.setEnd(Integer.parseInt(shardProp.getProperty("end")));

                    LOG.info(indexId + " shard " + range.getStart() + "->" + range.getEnd() + " path: " + range.getShardPath());

                    SHARD_RANGE_MAP.put(range, new Tuple<ShardRange, SerializationWriter>(range, null));
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }


    public SerializationWriter get(String shardId) {
        int hashCode = StringHash.murmurhash3_x86_32(shardId, 0, shardId.length(), 0);
        int microShard = Math.abs(hashCode % shardPartitions);

        int start = (microShard / shardStep * (shardStep - 1)) + microShard / shardStep;
        int end = start + shardStep;


        ShardRange shardRange = new ShardRange(start, end);
        Tuple<ShardRange, SerializationWriter> tuple = SHARD_RANGE_MAP.get(shardRange);
        shardRange = tuple.getKey();
        SerializationWriter writer = tuple.getValue();


        if(writer == null) {
            try {
                Path shardDataPath = new Path(shardRange.getShardPath(), "data");
                //FileSystem fileSystem = HadoopUtil.getFileSystem(new URI("file:///Users"));
                FileSystem fileSystem = HadoopUtil.getFileSystem(shardDataPath);

                String fileName;
                DateTime now = DateTime.now();
                if(StringUtils.isNotBlank(filePrefix)) {
                    fileName = filePrefix + "-" + now.toString("yyMMddHHmmss") + "-data.dat";
                } else {
                    fileName = "unknown-" + now.toString("yyMMddHHmmss") + "-data.dat";
                }

                Path path = new Path(shardDataPath, fileName);
                IntLengthHeaderFile.Writer fileWriter = new IntLengthHeaderFile.Writer(
                        fileSystem, path);

                /*
                String shardMetaFileName = indexName + ".shard." + shardRange.getShardName() + ".meta.properties";
                FSDataOutputStream out = fileSystem.create(new Path(shardDataPath, shardMetaFileName), true);
                IOUtils.write(info, out);
                IOUtils.closeQuietly(out);
                */
                writer = new SerializationWriter(fileWriter);

                LOG.info("new file: " + path);
                tuple.setValue(writer);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        return writer;
    }



    @Override
    public void write(String shardId, ByteBuffer objByte) {
        try {
            SerializationWriter writer = get(shardId);
            writer.write(objByte);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        for (Map.Entry<ShardRange, Tuple<ShardRange, SerializationWriter>> entry : SHARD_RANGE_MAP.entrySet()) {
            try {
                SerializationWriter value = entry.getValue().getValue();
                if(value != null) {
                    value.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public int getShardNum() {
        return shardNum;
    }

    public int getShardStep() {
        return shardStep;
    }

    public int getShardPartitions() {
        return shardPartitions;
    }

    public Path getIndexDataStoragePath() {
        return indexDataStoragePath;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getFilePrefix() {
        return filePrefix;
    }


    public static void main(String[] args) throws Exception {
        ZkConfiguration zkConf = new ZkConfiguration();

        ZkClient zkClient = new ZkClient(zkConf.getZKServers());


        InteractionProtocol protocol = new InteractionProtocol(zkClient, zkConf);
        MasterConfiguration masterConf = new MasterConfiguration();
        masterConf.setProperty("katta.master.code", "bggtf09-ojih65f");

        DefaultDataWriter writer = new DefaultDataWriter();
        writer.init(masterConf, protocol, "hello");
        writer.write("hello", ByteBuffer.wrap("javafdjasflkajsdlkfa".getBytes()));
    }
}
