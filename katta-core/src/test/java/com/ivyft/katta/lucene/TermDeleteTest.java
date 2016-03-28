package com.ivyft.katta.lucene;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.junit.After;
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
public class TermDeleteTest {


    protected String indexPath = "../data/katta-shards/0.0.0.0_20000/userindex#2PD95Ggl2tWnSynu8gX";



    IndexReader indexReader;


    IndexSearcher indexSearcher;

    public TermDeleteTest() {
    }


    @Before
    public void setUp() throws Exception {
        FSDirectory directory = FSDirectory.open(new File(indexPath));
        indexReader = DirectoryReader.open(directory);

        indexSearcher = new IndexSearcher(indexReader);
    }


    @After
    public void tearDown() throws Exception {
        indexReader.close();

    }

    @Test
    public void testSearch() throws Exception {
        System.out.println(indexSearcher.getIndexReader().numDocs());

        TopDocs word = indexSearcher.search(new TermQuery(new Term("word", "hello-10")), 10);
        System.out.println(word.totalHits);
    }
}
