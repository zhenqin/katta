package com.ivyft.katta.lucene;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/26
 * Time: 17:28
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class IndexReaderTest {


    protected String indexPath = "./data/katta-shards/zhenqin-pro102_20000/test#1FWYK0kjOUjzCy9KU71";



    IndexReader indexReader;


    IndexWriter indexWriter;

    public IndexReaderTest() {
    }


    @Before
    public void setUp() throws Exception {
        FSDirectory directory = FSDirectory.open(new File(indexPath));
        indexReader = DirectoryReader.open(directory);

        IndexWriterConfig writerConfig = new IndexWriterConfig(Version.LUCENE_46, new StandardAnalyzer(Version.LUCENE_46));
        writerConfig.getMergeScheduler();

        indexWriter = new IndexWriter(directory, writerConfig);
    }


    @Test
    public void testReader() throws Exception {
        indexWriter.addIndexes(FSDirectory.open(new File(indexPath)));

    }
}
