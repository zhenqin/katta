package com.ivyft.katta.lucene;

import com.ivyft.katta.lib.lucene.collector.FetchDocumentCollector;
import com.ivyft.katta.lib.lucene.convertor.SolrDocumentConvertor;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/3/21
 * Time: 09:27
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class IndexSearcherTest {



    protected String indexPath = "../data/katta-shards/zhenqin-pro102_20020/userindex#2PD95Ggl2tWnSynu8gX";



    IndexReader indexReader;


    IndexSearcher indexSearcher;

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
    public void testSortSearch() throws Exception {
        System.out.println(indexReader.numDocs());
        TopDocs topDocs = indexSearcher.search(new MatchAllDocsQuery(), 10);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            Document doc = indexSearcher.doc(scoreDoc.doc);
            System.out.println(doc);
        }

        System.out.println("=============================");

        //Sort
        int limit = 10;
        int offset = 0;

        Sort sort = new Sort(new SortField("CNT_FOLLOWINGS", SortField.Type.INT, true));
        TopDocsCollector topDocsCollector = TopFieldCollector.create(sort, limit, true, false, false, false);

        indexSearcher.search(new MatchAllDocsQuery(), topDocsCollector);

        SolrDocumentConvertor convertor = new SolrDocumentConvertor();
        topDocs = topDocsCollector.topDocs(offset, limit);
        scoreDocs = topDocs.scoreDocs;

        System.out.println(scoreDocs.length);
        for (ScoreDoc scoreDoc : scoreDocs) {
            Document doc = indexSearcher.doc(scoreDoc.doc);
            System.out.println(convertor.convert(doc, 1.0f));
        }


        System.out.println(sort);
        System.out.println("********************************");
    }
}
