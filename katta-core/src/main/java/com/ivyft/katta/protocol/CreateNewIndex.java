package com.ivyft.katta.protocol;

import com.ivyft.katta.protocol.metadata.NewIndexMetaData;
import com.ivyft.katta.protocol.metadata.Shard;
import com.ivyft.katta.util.HadoopUtil;
import com.ivyft.katta.util.UUIDCreator;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/4/22
 * Time: 14:31
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class CreateNewIndex {


    /**
     * Data Storage Path Prefix
     */
    protected final String dataStoragePath;


    /**
     * 新 Data Index
     */
    protected final NewIndexMetaData newIndexMetaData;


    protected static Logger LOG = LoggerFactory.getLogger(CreateNewIndex.class);


    /**
     * 构造方法
     * @param dataStoragePath 文件存放路径前缀
     * @param newIndexMetaData new Index
     */
    public CreateNewIndex(String dataStoragePath, NewIndexMetaData newIndexMetaData) {
        this.dataStoragePath = dataStoragePath;
        this.newIndexMetaData = newIndexMetaData;
    }



    /**
     * 走起
     */
    public void created() throws IOException {
        FileSystem fs = HadoopUtil.getFileSystem();

        String indexId = newIndexMetaData.getName();
        String indexShardNum = "katta.index." + indexId + ".shard.num";
        String indexShardStep = "katta.index." + indexId + ".shard.step";
        String indexShardPartition = "katta.index." + indexId + ".numPartitions";

        Path indexDataStoragePath = new Path(dataStoragePath + "/" + indexId);
        fs.mkdirs(indexDataStoragePath);
        LOG.info(indexId + " index data storage path: " + indexDataStoragePath);


        //在 index 下写入该 Index 的 shardNum 信息，shardStep 信息
        Path indexMetaPath = new Path(indexDataStoragePath, indexId + ".meta.properties");
        int shardPartitions = newIndexMetaData.getShardNum() * newIndexMetaData.getShardStep();

        Properties indexProp = new Properties();
        indexProp.setProperty("index.name", indexId);
        indexProp.setProperty(indexShardStep, String.valueOf(this.newIndexMetaData.getShardStep()));
        indexProp.setProperty(indexShardNum, String.valueOf(this.newIndexMetaData.getShardNum()));
        indexProp.setProperty(indexShardPartition, String.valueOf(shardPartitions));

        FSDataOutputStream outputStream = fs.create(indexMetaPath, true);
        try {
            indexProp.store(outputStream, "UTF-8");
            outputStream.flush();
        } finally {
            outputStream.close();
        }

        for (int i = 0; i < newIndexMetaData.getShardNum(); i++) {
            String shardId = UUIDCreator.uuid();
            Path shardDataStoragePath = new Path(indexDataStoragePath, shardId + "/data");
            fs.mkdirs(shardDataStoragePath);

            Path shardIndexStoragePath = new Path(indexDataStoragePath, shardId + "/index");
            fs.mkdirs(shardIndexStoragePath);

            LOG.info(indexId + " shard data " + shardId + " storage path: " + shardDataStoragePath);

            Properties shardProp = new Properties();
            shardProp.setProperty("start", String.valueOf(i * newIndexMetaData.getShardStep()));
            shardProp.setProperty("end", String.valueOf((i + 1) * newIndexMetaData.getShardStep()));
            shardProp.setProperty("shard", shardId);
            shardProp.setProperty("index", indexId);
            shardProp.setProperty("step", String.valueOf(newIndexMetaData.getShardStep()));
            shardProp.setProperty("total.partitions", String.valueOf(shardPartitions));

            String shardMetaFileName = indexId + ".shard." + shardId + ".meta.properties";
            //在 Shard 目录下写入 Shard 的信息
            FSDataOutputStream out = fs.create(new Path(shardDataStoragePath.getParent(), shardMetaFileName), true);

            try {
                shardProp.store(out, "UTF-8");
                out.flush();
            } finally {
                IOUtils.closeQuietly(out);
            }


            Shard shard = new Shard(shardId, shardDataStoragePath.toString());
            for (Map.Entry<Object, Object> entry : shardProp.entrySet()) {
                shard.put((String)entry.getKey(), (String)entry.getValue());
            }
            newIndexMetaData.addShard(shard);

        }


    }
}
