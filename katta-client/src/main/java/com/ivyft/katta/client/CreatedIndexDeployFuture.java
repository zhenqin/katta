package com.ivyft.katta.client;

import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.IndexDeployError;
import com.ivyft.katta.protocol.metadata.NewIndexMetaData;
import com.ivyft.katta.protocol.metadata.Shard;
import com.ivyft.katta.util.HadoopUtil;
import com.ivyft.katta.util.UUIDCreator;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/7
 * Time: 09:10
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class CreatedIndexDeployFuture implements IIndexDeployFuture, Runnable {


    protected InteractionProtocol protocol;




    protected NewIndexMetaData newIndexMetaData;




    protected IndexState indexState = IndexState.DEPLOYING;


    /**
     *
     */
    protected CountDownLatch countDownLatch = new CountDownLatch(1);



    protected boolean operating = false;


    protected ExecutorService executor = Executors.newSingleThreadExecutor();


    /**
     * 构造方法
     * @param protocol
     * @param meta
     */
    public CreatedIndexDeployFuture(InteractionProtocol protocol, NewIndexMetaData meta) {
        this.protocol = protocol;
        this.newIndexMetaData = meta;
    }




    @Override
    public void run() {
        try {
            String dataStoragePath = newIndexMetaData.getPath();

            FileSystem fs = HadoopUtil.getFileSystem();

            String indexId = newIndexMetaData.getName();
            String indexShardNum = "katta.index." + indexId + ".shard.num";
            String indexShardStep = "katta.index." + indexId + ".shard.step";
            String indexShardPartition = "katta.index." + indexId + ".numPartitions";

            Path indexDataStoragePath = new Path(dataStoragePath + "/" + indexId);
            fs.mkdirs(indexDataStoragePath);
            System.out.println(indexId + " index data storage path: " + indexDataStoragePath);


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

                System.out.println(indexId + " shard data " + shardId + " storage path: " + shardDataStoragePath);

                Properties shardProp = new Properties();
                shardProp.setProperty("start", String.valueOf(i * newIndexMetaData.getShardStep()));
                shardProp.setProperty("end", String.valueOf((i + 1) * newIndexMetaData.getShardStep()));
                shardProp.setProperty("shard", shardId);
                shardProp.setProperty("index", indexId);
                shardProp.setProperty("step", String.valueOf(newIndexMetaData.getShardStep()));
                shardProp.setProperty("total.partitions", String.valueOf(shardPartitions));

                String shardMetaFileName = indexId + ".shard." + shardId + ".meta.properties";
                FSDataOutputStream out = fs.create(new Path(shardDataStoragePath, shardMetaFileName), true);

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


            //尝试创建索引
            protocol.createIndex(newIndexMetaData);

            setIndexState(IndexState.DEPLOYED);
        } catch (IOException e) {
            IndexDeployError error = new IndexDeployError(newIndexMetaData.getName(), IndexDeployError.ErrorType.UNKNOWN);
            error.setException(e);

            newIndexMetaData.setDeployError(error);
            setIndexState(IndexState.ERROR);
        } finally {
            try {
                executor.shutdown();
            } catch (Exception e) {

            }
            countDownLatch.countDown();
        }

    }


    @Override
    public IndexState joinDeployment() throws InterruptedException {
        return joinDeployment(Integer.MAX_VALUE);
    }

    @Override
    public IndexState joinDeployment(long maxWaitMillis) throws InterruptedException {
        if(!operating) {
            operating = true;
            try {
                executor.submit(this);
            } catch (Exception e){
                e.printStackTrace();
            }
        }

        try {
            countDownLatch.await(maxWaitMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {

        }
        return getState();
    }


    public synchronized void setIndexState(IndexState indexState) {
        this.indexState = indexState;
    }


    @Override
    public IndexState getState() {
        return indexState;
    }

}
