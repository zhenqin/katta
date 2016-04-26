package com.ivyft.katta.node;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
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
    protected final File indexPath;


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



    public LuceneIndexMergeManager(File indexPath,
                                   String indexName,
                                   String shardName,
                                   IndexUpdateListener updateListener) {
        this(indexPath, indexName, shardName, updateListener, new StandardAnalyzer(Version.LUCENE_46));
    }



    public LuceneIndexMergeManager(File indexPath,
                                   String indexName,
                                   String shardName,
                                   IndexUpdateListener updateListener,
                                   Analyzer analyzer) {
        this.indexPath = indexPath;
        this.indexName = indexName;
        this.shardName = shardName;
        this.updateListener = updateListener;
        try {
            FSDirectory open = FSDirectory.open(indexPath);

            masterIndexWriter = new IndexWriter(open, new IndexWriterConfig(Version.LUCENE_46, analyzer));

            indexSearcher = new IndexSearcher(DirectoryReader.open(masterIndexWriter, true));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }


    /**
     * 把 shardFolder 合并到主索引库
     * @param shardFolder 临时索引库
     * @throws IOException
     */
    public void mergeIndex(File shardFolder) throws IOException {
        LOG.info("deleted success, {} docs count {}", indexPath.getName(), masterIndexWriter.numDocs());
        LOG.info("merge index {} to {}", shardFolder.getName(), indexPath.getName());
        FSDirectory open = FSDirectory.open(shardFolder);
        try {
            masterIndexWriter.addIndexes(open);
            LOG.info("merged success, {} num docs {}", indexPath.getName(), masterIndexWriter.numDocs());
        } finally {
            open.close();
        }

        //写一个标志文件，表示索引已经被改变了。
        File file = new File(indexPath, WRITED_DATA_FILE);
        if(!file.exists()) {
            file.createNewFile();
        }

        file.setLastModified(System.currentTimeMillis());
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
        LOG.info("{} docs count {}", indexPath.getName(), masterIndexWriter.numDocs());
        if(deleteDocsCouter.get() > 0 || addDocsCouter.get() > 0) {
            LOG.info("delete num docs {}, add num doc {}, and commit.", deleteDocsCouter.get(), addDocsCouter.get());
            masterIndexWriter.commit();
        }


        if(deleteDocsCouter.get() > 0) {
            masterIndexWriter.forceMergeDeletes(true);
        }

        //写一个标志文件，表示索引已经被改变了。
        File file = new File(indexPath, WRITED_DATA_FILE);
        if(!file.exists()) {
            file.createNewFile();
        }

        file.setLastModified(System.currentTimeMillis());
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

        masterIndexWriter.forceMerge(seg, false);
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
            LOG.info("close index writer " + indexPath.getName());
            masterIndexWriter.close(true);
        }

    }
}
