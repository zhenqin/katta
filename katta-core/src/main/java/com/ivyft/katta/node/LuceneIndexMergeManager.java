package com.ivyft.katta.node;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

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



    protected static Logger LOG = LoggerFactory.getLogger(LuceneIndexMergeManager.class);



    public LuceneIndexMergeManager(File indexPath) {
        this(indexPath, new StandardAnalyzer(Version.LUCENE_46));
    }



    public LuceneIndexMergeManager(File indexPath, Analyzer analyzer) {
        this.indexPath = indexPath;
        try {
            indexWriter = new IndexWriter(FSDirectory.open(indexPath), new IndexWriterConfig(Version.LUCENE_46, analyzer));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }




    public void mergeIndex(File shardFolder) {
        try {
            LOG.info("{} max docs count {}", indexPath.getAbsolutePath(), indexWriter.maxDoc());
            LOG.info("merge index {} to {}", shardFolder.getAbsolutePath(), indexPath.getAbsolutePath());
            indexWriter.addIndexes(FSDirectory.open(shardFolder));
            LOG.info("merged success, {} max docs num {}", indexPath.getAbsolutePath(), indexWriter.maxDoc());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }




    public void optimize(int seg) throws IOException {
        //30W 以下5个段，以上每加 30W 增加一个段
        int num = indexWriter.maxDoc();
        seg = seg + (num / 300000);

        indexWriter.forceMerge(seg, false);
    }




    @Override
    public void close() throws IOException {
        if(indexWriter != null) {
            LOG.info("close index writer " + indexPath.getAbsolutePath());
            indexWriter.close(true);
        }
    }
}
