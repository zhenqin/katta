package com.ivyft.katta.node;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
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




    protected final File indexPath;




    protected final IndexWriter indexWriter;


    /**
     * 删除时,可以确定删除数据量
     */
    protected final IndexSearcher indexSearcher;

    /**
     * 删除索引的计数器
     */
    protected final AtomicLong deleteDocs = new AtomicLong(0);


    protected static Logger LOG = LoggerFactory.getLogger(LuceneIndexMergeManager.class);



    public LuceneIndexMergeManager(File indexPath) {
        this(indexPath, new StandardAnalyzer(Version.LUCENE_46));
    }



    public LuceneIndexMergeManager(File indexPath, Analyzer analyzer) {
        this.indexPath = indexPath;
        try {
            FSDirectory open = FSDirectory.open(indexPath);

            indexWriter = new IndexWriter(open, new IndexWriterConfig(Version.LUCENE_46, analyzer));

            indexSearcher = new IndexSearcher(DirectoryReader.open(indexWriter, true));
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
        if(deleteDocs.get() > 0) {
            LOG.info("delete docs {}", deleteDocs.get());
            indexWriter.commit();
        }


        LOG.info("{} max docs count {}", indexPath.getName(), indexWriter.maxDoc());
        LOG.info("merge index {} to {}", shardFolder.getName(), indexPath.getName());
        indexWriter.addIndexes(FSDirectory.open(shardFolder));
        LOG.info("merged success, {} max docs num {}", indexPath.getName(), indexWriter.maxDoc());
    }


    /**
     * 删除旧数据,索引
     * @param field
     * @param id
     * @throws IOException
     */
    public int delete(String field, String id) throws IOException {
        Term term = new Term(field, id);
        TopDocs topDocs = indexSearcher.search(new TermQuery(term), 1);
        if(topDocs.totalHits > 0) {
            indexWriter.deleteDocuments(term);
            deleteDocs.incrementAndGet();
        }
        return topDocs.totalHits;
    }


    /**
     * 删除旧数据索引.
     * @param query
     * @throws IOException
     */
    public int delete(Query query) throws IOException {
        TopDocs topDocs = indexSearcher.search(query, 1);
        if(topDocs.totalHits > 0) {
            indexWriter.deleteDocuments(query);
            deleteDocs.incrementAndGet();
        }
        return topDocs.totalHits;

    }



    public void commit() throws IOException {
        indexWriter.commit();
    }


    public void optimize(int seg) throws IOException {
        //30W 以下5个段，以上每加 30W 增加一个段
        int num = indexWriter.maxDoc();
        seg = seg + (num / 300000);

        indexWriter.forceMerge(seg, false);
    }




    @Override
    public void close() throws IOException {
        try {
            if(indexSearcher != null) {
                indexSearcher.getIndexReader().close();
            }
        } catch (IOException e) {
            LOG.warn(e.getMessage());
        }

        if(indexWriter != null) {
            LOG.info("close index writer " + indexPath.getName());
            indexWriter.close(true);
        }

    }
}
