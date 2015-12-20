package com.ivyft.katta.lib.writer;

import com.ivyft.katta.protocol.*;
import com.ivyft.katta.util.*;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.*;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

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
public class DefaultDataWriter extends DataWriter implements Runnable {


    private int shardNum;


    private int shardStep;


    private int shardPartitions;


    protected Path indexDataStoragePath;


    protected String indexName = "unknown-index";



    protected String filePrefix = "";



    protected String fileSuffix = ".dat";


    protected Map<ShardRange, Tuple<ShardRange, SerializationWriter>>
            SHARD_RANGE_MAP = new HashMap<ShardRange, Tuple<ShardRange, SerializationWriter>>();


    protected final ReentrantLock LOCK = new ReentrantLock();



    protected static Logger LOG = LoggerFactory.getLogger(DefaultDataWriter.class);


    public DefaultDataWriter() {
    }

    @Override
    public void init(MasterConfiguration conf, InteractionProtocol protocol, String indexId) {
        this.indexName = indexId;
        this.filePrefix = conf.getString("katta.master.code", "");
        this.fileSuffix = conf.getString("katta.writer.file.suffix", ".dat");
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

            LOG.info("indexId: "+indexId+", shardNum: "+shardNum+", shardStep: "+
                    shardStep+", shardPartitions: " + shardPartitions);

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
                        IOUtils.closeQuietly(open);
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


    /**
     * 根据 RowKey 获得数据进入哪个分区, 并返回分区的数据 Writer
     * @param shardId RowKey
     * @return 返回数据分区的 Writer
     */
    protected SerializationWriter get(String shardId) {
        int hashCode = StringHash.murmurhash3_x86_32(shardId, 0, shardId.length(), 0);
        int microShard = Math.abs(hashCode % shardPartitions);

        //int start = (microShard / shardStep * (shardStep - 1)) + microShard / shardStep;
        /*
         * 对于任意的 microShard, 该算法计算数据能落入到哪个区间
         */
        int start = microShard - microShard % shardStep;

        int end = start + shardStep;


        ShardRange shardRange = new ShardRange(start, end);
        Tuple<ShardRange, SerializationWriter> tuple = SHARD_RANGE_MAP.get(shardRange);

        //ShardRange 已经重写了 hashCode 方法, 这里已经可以完整的 ShardRange
        shardRange = tuple.getKey();
        SerializationWriter writer = tuple.getValue();

        if(writer == null || writer.isClosed()) {
            if(writer != null && writer.isClosed()) {
                try {
                    Path shardDataPath = new Path(shardRange.getShardPath(), "data");
                    //FileSystem fileSystem = HadoopUtil.getFileSystem(new URI("file:///Users"));
                    FileSystem fs = HadoopUtil.getFileSystem(shardDataPath);

                    Path filePath = writer.getFilePath();
                    IntLengthHeaderFile.Writer fileWriter = new IntLengthHeaderFile.Writer(
                            fs, filePath, false);

                    writer = new SerializationWriter(fileWriter, filePath);

                    LOG.info("new file: " + filePath);
                    tuple.setValue(writer);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            } else {
                try {
                    Path shardDataPath = new Path(shardRange.getShardPath(), "data");
                    //FileSystem fileSystem = HadoopUtil.getFileSystem(new URI("file:///Users"));
                    FileSystem fs = HadoopUtil.getFileSystem(shardDataPath);

                    String fileName;
                    DateTime now = DateTime.now();
                    if(StringUtils.isNotBlank(filePrefix)) {
                        fileName = filePrefix + "-" + now.toString("yyyyMMddHHmmss") + fileSuffix;
                    } else {
                        fileName = "unknown-" + now.toString("yyyyMMddHHmmss") + fileSuffix;
                    }

                    Path path = new Path(shardDataPath, fileName);
                    IntLengthHeaderFile.Writer fileWriter = new IntLengthHeaderFile.Writer(
                            fs, path);

                    writer = new SerializationWriter(fileWriter, path);

                    LOG.info("new file: " + path);
                    tuple.setValue(writer);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }
        return writer;
    }


    @Override
    public void write(String shardId, ByteBuffer objByte) {
        //如果当前正在 Commit, 或者由修复 IndexWriter 行为, 需要锁定
        if(LOCK.isLocked()) {
            synchronized (this) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        try {
            SerializationWriter writer = get(shardId);
            writer.write(objByte);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }


    @Override
    public void flush() {
        for (Map.Entry<ShardRange, Tuple<ShardRange, SerializationWriter>> entry : SHARD_RANGE_MAP.entrySet()) {
            try {
                SerializationWriter value = entry.getValue().getValue();
                if(value != null && value.isOpened()) {
                    value.flush();

                    LOG.info("katta index "+indexName+" shard "+entry.getKey().getShardName()+" flushed.");
                }
            } catch (IOException e) {
                LOG.error("katta index " + indexName + " index data writer flush error.", e);
            }
        }
    }



    @Override
    public void close() {
        for (Map.Entry<ShardRange, Tuple<ShardRange, SerializationWriter>> entry : SHARD_RANGE_MAP.entrySet()) {
            try {
                SerializationWriter value = entry.getValue().getValue();
                if(value != null && value.isOpened()) {
                    value.close();
                }
            } catch (IOException e) {
                LOG.error("close katta index " + indexName + " error.", e);
            }
        }
    }



    public Collection<ShardRange> listShardRanges() {
        Set<ShardRange> shardRanges = new HashSet<ShardRange>(SHARD_RANGE_MAP.size());
        for (Map.Entry<ShardRange, Tuple<ShardRange, SerializationWriter>> entry : SHARD_RANGE_MAP.entrySet()) {
            if(entry.getValue().getValue() != null && entry.getValue().getValue().isOpened()) {
                shardRanges.add(entry.getKey());
            }
        }

        return shardRanges;
    }


    /**
     * 重置索引, 当在 Commit 后, 要锁定索引
     *
     * @param actionName ROLLBACK OR COMMIT
     *
     */
    @Override
    protected Set<ShardRange> reset(String actionName, String commitId) {
        Set<ShardRange> aShardRanges = new HashSet<ShardRange>(SHARD_RANGE_MAP.size());
        LOCK.lock();
        try {
            LOG.info(this.indexName + " index data writer locked");
            close();

            //do something

            for (Map.Entry<ShardRange, Tuple<ShardRange, SerializationWriter>> entry : SHARD_RANGE_MAP.entrySet()) {
                Tuple<ShardRange, SerializationWriter> value = entry.getValue();
                if(value.getValue() != null && value.getValue().isClosed()) {
                    ShardRange shardRange = entry.getKey();

                    FileSystem fs = HadoopUtil.getFileSystem();
                    Path shardDataPath = new Path(shardRange.getShardPath(), "data");
                    Path commitTimeLinePath = new Path(shardRange.getShardPath(), actionName + commitId);

                    //把 shard data 目录重命名成 commit-20151010121230 的目录, 表示一个提交点
                    fs.rename(shardDataPath, commitTimeLinePath);
                    LOG.info("new commit batch: " + commitTimeLinePath.toString());

                    //再次创建 shard data 目录
                    if (!fs.exists(shardDataPath)) {
                        fs.mkdirs(shardDataPath);
                        LOG.info("mkdir: " + shardDataPath.toString());
                    }

                    //再次把 shard properties 文件 copy 到 data 目录下, 使 shard data 完整
                    String shardMetaFileName = this.indexName + ".shard." + shardRange.getShardName() + ".meta.properties";
                    Path shardPropPath = new Path(shardDataPath, shardMetaFileName);

                    FSDataInputStream in = fs.open(new Path(commitTimeLinePath, shardMetaFileName));
                    FSDataOutputStream out = null;
                    try {
                        out = fs.create(shardPropPath, true);
                        IOUtils.copy(in, out);
                        out.flush();
                    } finally {
                        IOUtils.closeQuietly(in);
                        IOUtils.closeQuietly(out);
                    }

                    //使用 Clone, 防止 Map 的原始 ShardRange 被改变
                    ShardRange commitShardRange = (ShardRange) shardRange.clone();
                    commitShardRange.setShardPath(commitTimeLinePath.toString());

                    aShardRanges.add(commitShardRange); //一个提交点
                    value.setValue(null); //这里置为 null, 下次会创新创建 Data File
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            LOCK.unlock();
            LOG.info(this.indexName + " index data writer unlocked");
            synchronized (this) {
                this.notifyAll();
            }
        }

        return aShardRanges;
    }




    @Override
    public void run() {
        try {
            Thread.sleep(2000);

            System.out.println("************************");
            //以后可以检查那些 DataWriter 长时间不用的, close
            System.out.println("========================");
        } catch (InterruptedException e) {
            e.printStackTrace();
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


    public String getFileSuffix() {
        return fileSuffix;
    }

    public static void main(String[] args) throws Exception {
        ZkConfiguration zkConf = new ZkConfiguration();

        ZkClient zkClient = new ZkClient(zkConf.getZKServers());


        InteractionProtocol protocol = new InteractionProtocol(zkClient, zkConf);
        MasterConfiguration masterConf = new MasterConfiguration();
        masterConf.setProperty("katta.master.code", "bggtf09-ojih65f");

        DefaultDataWriter writer = new DefaultDataWriter();
        writer.init(masterConf, protocol, "hello");
        for (int i = 0; i < 1000; i++) {
            writer.write("java" + i, ByteBuffer.wrap(("hello" + new Random().nextInt()).getBytes()));
        }

        String now = DateTime.now().toString("yyyyMMddHHmmss");
        writer.commit(now);
        System.out.println(writer.listShardRanges());

        for (int i = 0; i < 1000; i++) {
            writer.write("java" + i, ByteBuffer.wrap(("hello" + new Random().nextInt()).getBytes()));
        }

//        Thread lockTest = new Thread(writer);
//        lockTest.start();

        //writer.commit();
    }
}
