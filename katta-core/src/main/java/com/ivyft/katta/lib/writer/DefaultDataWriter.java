package com.ivyft.katta.lib.writer;

import com.ivyft.katta.protocol.IntLengthHeaderFile;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.HadoopUtil;
import com.ivyft.katta.util.MasterConfiguration;
import com.ivyft.katta.util.StringHash;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
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


    private int shardNum = 3;


    private int shardStep;


    private int shardPartitions;


    protected Path indexDataStoragePath;


    protected String indexName = "unknown-index";



    protected String filePrefix = "";



    protected Map<Integer, SerializationWriter> writerMap = new HashMap<Integer, SerializationWriter>();


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


        String dataStoragePath = conf.getString("katta.blck.data.storage.path");
        //String dataStoragePath = "/user/katta/data";

        FileSystem fileSystem = null;
        try {
            this.indexDataStoragePath = new Path(dataStoragePath + "/" + indexId);
            //fileSystem = HadoopUtil.getFileSystem(new URI("file:///Users"));
            fileSystem = HadoopUtil.getFileSystem(indexDataStoragePath);
            fileSystem.mkdirs(indexDataStoragePath);
            LOG.info(indexId + " index data storage path: " + indexDataStoragePath);


            Path indexMetaPath = new Path(this.indexDataStoragePath, indexId + ".meta.properties");
            if (fileSystem.exists(indexMetaPath) && fileSystem.isFile(indexMetaPath)) {
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
            } else {
                //新创建的索引
                this.shardNum = conf.getInt(indexShardNum, 3);
                this.shardStep = conf.getInt(indexShardStep, 5);

                this.shardPartitions = shardNum * shardStep;

                Properties metaProp = new Properties();
                metaProp.setProperty(indexShardStep, String.valueOf(this.shardStep));
                metaProp.setProperty(indexShardNum, String.valueOf(this.shardNum));
                metaProp.setProperty(indexShardPartition, String.valueOf(this.shardPartitions));

                FSDataOutputStream outputStream = fileSystem.create(indexMetaPath, true);
                try {
                    metaProp.store(outputStream, "UTF-8");
                    outputStream.flush();
                } finally {
                    outputStream.close();
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }


    public SerializationWriter get(String shardId) {
        int hashCode = StringHash.murmurhash3_x86_32(shardId, 0, shardId.length(), 0);
        int microShard = Math.abs(hashCode % shardNum);
        SerializationWriter writer = writerMap.get(microShard);
        //IntLengthHeaderFile.Writer writer = writerMap.get(microShard);
        if(writer == null) {
            try {
                Path parent = new Path(this.indexDataStoragePath, String.valueOf(microShard));
                //FileSystem fileSystem = HadoopUtil.getFileSystem(new URI("file:///Users"));
                FileSystem fileSystem = HadoopUtil.getFileSystem(parent);
                fileSystem.mkdirs(parent);

                String fileName;
                DateTime now = DateTime.now();
                if(StringUtils.isNotBlank(filePrefix)) {
                    fileName = filePrefix + "-" + now.toString("yyMMddHHmmss") + "-data.dat";
                } else {
                    fileName = "unknown-" + now.toString("yyMMddHHmmss") + "-data.dat";
                }

                IntLengthHeaderFile.Writer fileWriter = new IntLengthHeaderFile.Writer(
                        fileSystem, new Path(parent, fileName));

                String info = "start=" + (microShard * shardStep) + "\n" +
                        "end=" + (microShard * shardStep + shardStep);

                String shardMetaFileName = indexName + ".shard." + microShard + ".meta.properties";
                FSDataOutputStream out = fileSystem.create(new Path(parent, shardMetaFileName), true);
                IOUtils.write(info, out);
                IOUtils.closeQuietly(out);

                writer = new SerializationWriter(fileWriter);
                writerMap.put(microShard, writer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return writer;
    }



    @Override
    public void write(String shardId, ByteBuffer objByte) {
        //Object deserialize = serializer.deserialize(objByte.array());
        System.out.println(shardId);
        try {
            get(shardId).write(objByte);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        for (SerializationWriter writer : writerMap.values()) {
            try {
                writer.close();
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
}
