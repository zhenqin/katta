package com.ivyft.katta.hadoop;


import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.ivyft.katta.util.UUIDCreator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 *
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-10-31
 * Time: 上午8:38
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class LuceneOutputWriter {


    private final AtomicInteger commitInt = new AtomicInteger(0);


    private IndexWriter indexWriter;


    protected Configuration configuration;

    private Path outputPath;
    private File temp;
    private FileSystem fs;


    private static Logger LOG = LoggerFactory.getLogger(LuceneOutputWriter.class);


    public LuceneOutputWriter() {

    }

    public void open(JobContext job) throws IOException {
        this.configuration = job.getConfiguration();
        this.fs = FileSystem.get(this.configuration);
        temp = new File(this.configuration.get("lucene.index.tmp.dir", "/tmp"), UUIDCreator.uuid());
        if(temp.exists() && temp.isDirectory()) {
            FileUtils.deleteDirectory(temp);
        }

        if(!temp.mkdirs()) {
            throw new IOException(temp.getAbsolutePath() + " can not create.");
        }

        outputPath = FileOutputFormat.getOutputPath(job);

        try {
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(
                    Version.LUCENE_46,
                    new StandardAnalyzer(Version.LUCENE_46));

            TieredMergePolicy mergePolicy = new TieredMergePolicy();

            //如果索引的段超过这个值,则永远不会被合并,默认5G, 这里2G
            mergePolicy.setMaxMergedSegmentMB(this.configuration.getInt("lucene.max.merged.segment.mb", 2048));

            /*
             * 一次合并最大合并多少个段？
             * 当索引段数小于这个数，则永不会被发生合并
             * 越大合并次数越少，但在发生合并后IO会很高。
             * 可能会造成停顿
             * 也就是说当索引段超过这个值, 可能会进行合并索引操作
             * 这个值只会用在前端的合并，默认值10。
             * 后台合并用setMaxMergeAtOnceExplicit
             */
            mergePolicy.setMaxMergeAtOnce(this.configuration.getInt("lucene.max.merge.at.once", 10));

            //同一次，最大合并的段数,这个一般在后台合并的值。 默认30
            mergePolicy.setMaxMergeAtOnceExplicit(this.configuration.getInt("lucene.max.merge.at.once.explicit", 30));

            /*
             * 为了防止频繁的flush引起小段，该值为了限制小段的大小。
             * 如果一个段到达这个值，则可能会加入到合并列表中。
             * 当索引段尺寸超过这个值,则合并. 默认2M
             */
            mergePolicy.setFloorSegmentMB(this.configuration.getInt("lucene.floor.segment.mb", 5));


           /*
            * 段总大小和最小段之间的比例，当超过该值则引起大量合并。默认10.0
            * 较小的值意味着更多的合并。但是段数会更少。
            * 这个值应该>=setMaxMergeAtOnce
            *
            * minSegmentBytes = Math.max(floorSegmentBytes, bytes);
            * double segCountLevel = totalIndexBytes / (double) minSegmentBytes;
            * if (segCountLevel < segsPerTier)
            *     allowedSegCount += Math.ceil(segCountLevel);
            */
            mergePolicy.setSegmentsPerTier(10.0);


            SerialMergeScheduler mergeScheduler = new SerialMergeScheduler();

            //当add的文档超过该值, 则刷新索引, 注意,他不是commit,只是把内存的数据刷写到磁盘
            indexWriterConfig.setMaxBufferedDocs(this.configuration.getInt("lucene.max.buffered.docs", 5000));
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

            //当add的文档在内润超过该值,则强制刷新索引, 如:setMaxBufferedDocs
            //这个方法一般用于比较大的文档.
            //该值和setMaxBufferedDocs一起起作用.达到任何一个要求则进行刷写
            indexWriterConfig.setRAMBufferSizeMB(this.configuration.getInt("lucene.max.buffered.size.mb", 32));

            //当删除文档数超过这个值,立即刷写. 默认-1(禁用这个功能)
            indexWriterConfig.setMaxBufferedDeleteTerms(-1);

            indexWriterConfig.setMergePolicy(mergePolicy);
            indexWriterConfig.setMergeScheduler(mergeScheduler);

            indexWriter = new IndexWriter(
                    FSDirectory.open(temp), indexWriterConfig);
        } catch (Exception e) {
            throw new RuntimeException("create indexWriter error: ", e);
        }
    }

   /*
    * @param luceneDoc
    *
    * 接受HDFSDocument对象，从中读取信息并建立索引
    */
    public void write(Document luceneDoc) throws IOException {

        // 如果使用Field.Index.ANALYZED选项，则默认情况下会对中文进行分词。
        // 如果这时候采用Term的形式进行检索，将会出现检索失败的情况。

        indexWriter.addDocument(luceneDoc);

        commitInt.incrementAndGet();
        if (commitInt.get() >= this.configuration.getInt("lucene.commit.counter", 10000)) {
            indexWriter.commit();
            commitInt.set(0);
        }
    }


    /**
     * 关闭IndexWriter
     * @throws IOException
     */
    public void close() throws IOException {
        LOG.info("a job success. commit and shutdown");
        if (commitInt.get() >= 0) {
            Future<Object> r = Executors.newSingleThreadExecutor().submit(
                    new Callable<Object>() {
                        @Override
                        public Object call() throws Exception {
                            try {
                                // 索引优化和IndexWriter对象关闭
                                indexWriter.commit();
                            } catch (Exception e) {
                                LOG.warn(e.getMessage(), e);
                            }
                            return new Object();
                        }
                    });

            try {
                r.get(this.configuration.getInt("lucene.shutdown.commit.timeout", 60 * 5), TimeUnit.SECONDS);
            } catch (Exception e) {
                LOG.warn(e.getMessage(), e);
            }

            try {
                indexWriter.close();
            } catch (Exception e) {
                LOG.warn(e.getMessage(), e);
            }
        }

        // 将本地索引结果拷贝到HDFS上
        fs.completeLocalOutput(outputPath, new Path("file://" + temp.getAbsolutePath()));
    }
}
