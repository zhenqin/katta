package com.ivyft.katta.node;

import com.ivyft.katta.util.HadoopUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/3/16
 * Time: 16:07
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class LuceneIndexMergeManager implements Closeable {


    /**
     * 如果一个索引已经被改变，需要写入一个文件，不能再被更改了
     */
    public final static String WRITED_DATA_FILE = ".ns.info";


    /**
     * Lucene 索引
     */
    protected final URI indexPath;


    /**
     * Lucene 主索引库的 IndexWriter
     */
    protected final IndexWriter masterIndexWriter;


    /**
     * 删除时,可以确定删除数据量
     */
    protected final IndexSearcher indexSearcher;


    /**
     * 删除索引的计数器
     */
    protected final AtomicLong deleteDocsCouter = new AtomicLong(0);



    /**
     * 索引被更新时回调
     */
    protected final IndexUpdateListener updateListener;



    /**
     * Merge Index  Name
     */
    protected final String indexName;


    /**
     * Merge Index-Shard Name
     */
    protected final String shardName;


    /**
     * ADD Doc 的计数器
     */
    protected final AtomicLong addDocsCouter = new AtomicLong(0);


    protected static Logger LOG = LoggerFactory.getLogger(LuceneIndexMergeManager.class);



    public LuceneIndexMergeManager(URI indexPath,
                                   String indexName,
                                   String shardName,
                                   IndexUpdateListener updateListener) {
        this(indexPath, indexName, shardName, updateListener, new StandardAnalyzer(Version.LUCENE_46));
    }



    public LuceneIndexMergeManager(URI indexPath,
                                   String indexName,
                                   String shardName,
                                   IndexUpdateListener updateListener,
                                   Analyzer analyzer) {
        this.indexPath = indexPath;
        this.indexName = indexName;
        this.shardName = shardName;
        this.updateListener = updateListener;
        String scheme = indexPath.getScheme() == null ? "file" : indexPath.getScheme();
        Directory directory = null;
        try {
            if (StringUtils.equals(ShardManager.HDFS, scheme)) {
                LOG.info("open hdfs index: " + indexPath.toString());
                Configuration hadoopConf = HadoopUtil.getHadoopConf();
                directory = new HdfsDirectory(new Path(indexPath), hadoopConf);
                masterIndexWriter = new IndexWriter(directory, new IndexWriterConfig(Version.LUCENE_46, analyzer));
            } else if (StringUtils.equals(ShardManager.FILE, scheme)) {
                LOG.info("open file index: " + indexPath.toString());
                directory = FSDirectory.open(new File(indexPath));
                masterIndexWriter = new IndexWriter(directory, new IndexWriterConfig(Version.LUCENE_46, analyzer));
            } else {
                throw new IllegalStateException("unknow schema " + scheme + " and path: " + indexPath.toString());
            }

            indexSearcher = new IndexSearcher(DirectoryReader.open(masterIndexWriter, true));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }


    /**
     * 把 shardFolder 合并到主索引库
     * @param shardFolder 临时索引库
     * @param mergedDelete 合并完成后，是否删除该 shardFolder
     * @throws IOException
     */
    public void mergeIndex(URI shardFolder, boolean mergedDelete) throws IOException {
        LOG.info("index {} docs count {}", indexName, masterIndexWriter.numDocs());
        String scheme = shardFolder.getScheme() == null ? "file" : shardFolder.getScheme();
        Directory directory = null;
        try {
            if (StringUtils.equals(ShardManager.HDFS, scheme)) {
                LOG.info("merge hdfs index: {} to {}", shardFolder.toString(), indexName);
                Configuration hadoopConf = HadoopUtil.getHadoopConf();
                directory = new HdfsDirectory(new Path(shardFolder), hadoopConf);
            } else if (StringUtils.equals(ShardManager.FILE, scheme)) {
                LOG.info("merge file index: {} to {} ", shardFolder.getPath(), indexName);
                directory = FSDirectory.open(new File(shardFolder.getPath()));
            } else {
                throw new IllegalStateException("unknow schema " + scheme + " and path: " + indexPath.toString());
            }

            masterIndexWriter.addIndexes(directory);
            LOG.info("merged success, {} num docs {}", indexName, masterIndexWriter.numDocs());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            if(directory != null) {
                directory.close();
            }
        }

        //写一个标志文件，表示索引已经被改变了。
        Path path = new Path(new Path(indexPath), WRITED_DATA_FILE);
        FileSystem fs = HadoopUtil.getFileSystem(path);

        if(!fs.exists(path)) {
            fs.createNewFile(path);
        }

        fs.getFileStatus(path);
        fs.setTimes(path, System.currentTimeMillis(), System.currentTimeMillis());

        // 合并完成后删除 shard
        if(mergedDelete) {
            LOG.warn("delete folder {}", shardFolder.getPath());
            fs.delete(new Path(shardFolder), true);
        }
    }




    /**
     * 把 doc ADD到主索引库
     *
     * @param doc Lucene Doc
     * @throws IOException
     */
    public void addDoc(Document doc) throws IOException {
        masterIndexWriter.addDocument(doc);
        addDocsCouter.incrementAndGet();
    }



    /**
     * 把 doc ADD到主索引库
     *
     * @param docs Lucene Docs
     * @throws IOException
     */
    public void addDocs(Collection<Document> docs) throws IOException {
        for (Document doc : docs) {
            masterIndexWriter.addDocument(doc);
            addDocsCouter.incrementAndGet();
        }
    }


    /**
     * 修改 Doc
     * @param term
     * @param doc
     * @throws IOException
     */
    public void updateDocs(Term term, Document doc) throws IOException {
        masterIndexWriter.updateDocument(term, doc);
        addDocsCouter.incrementAndGet();
    }



    /**
     * 删除旧数据,索引
     *
     * @param field
     *
     * @param id
     * @throws IOException
     */
    public int delete(String field, String id) throws IOException {
        Term term = new Term(field, id);
        return delete(new TermQuery(term));
    }


    /**
     * 删除旧数据索引.
     *
     * @param query
     *
     * @throws IOException
     */
    public int delete(Query query) throws IOException {
        TopDocs topDocs = indexSearcher.search(query, 1);
        if(topDocs.totalHits > 0) {
            masterIndexWriter.deleteDocuments(query);
            deleteDocsCouter.incrementAndGet();
        }
        return topDocs.totalHits;

    }


    /**
     * Commit Lucene Index
     * @throws IOException
     */
    public void commitAndDelete() throws IOException {
        LOG.info("{} docs count {}", indexName, masterIndexWriter.numDocs());
        if(deleteDocsCouter.get() > 0 || addDocsCouter.get() > 0) {
            LOG.info("delete num docs {}, add num doc {}, and commit.", deleteDocsCouter.get(), addDocsCouter.get());
            masterIndexWriter.commit();
        }


        if(deleteDocsCouter.get() > 0) {
            masterIndexWriter.forceMergeDeletes(true);
        }

        //写一个标志文件，表示索引已经被改变了。
        Path path = new Path(new Path(indexPath), WRITED_DATA_FILE);
        FileSystem fs = HadoopUtil.getFileSystem(path);

        if(!fs.exists(path)) {
            fs.createNewFile(path);
        }

        fs.getFileStatus(path);
        fs.setTimes(path, System.currentTimeMillis(), System.currentTimeMillis());
    }


    /**
     * 优化 Lucene 索引，到指定的段数量
     * @param seg
     * @throws IOException
     */
    public void optimize(int seg) throws IOException {
        //30W 以下5个段，以上每加 30W 增加一个段
        int num = masterIndexWriter.numDocs();
        seg = seg + (num / 300000);

        masterIndexWriter.forceMerge(seg, true);
    }


    /**
     * 关闭 Lucene 索引
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        updateListener.onAfterUpdate(indexName, shardName);
        try {
            if(indexSearcher != null) {
                indexSearcher.getIndexReader().close();
            }
        } catch (IOException e) {
            LOG.warn(e.getMessage());
        }

        if(masterIndexWriter != null) {
            LOG.info("close index writer " + indexName);
            masterIndexWriter.close(true);
        }

    }
}
