package com.ivyft.katta.lucene;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/3/24
 * Time: 15:54
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class SortDocSearchTest {



    protected String indexPath = "../data/test/1OgAEMwPu0snaXcdOhe";



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
    public void testCustomSort() throws Exception {
        Sort sort = new Sort(new SortField[]{
                new SortField("TITLE", new FieldComparatorSource() {
                    @Override
                    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                        return new TextComparator(numHits, fieldname, "NO");
                    }
                }, false)
        });

        TopDocs search = indexSearcher.search(new MatchAllDocsQuery(), 100, sort);
        System.out.println(search.totalHits);

        for(ScoreDoc doc : search.scoreDocs) {
            Document document = indexSearcher.doc(doc.doc);
            System.out.println(document.get("TITLE"));
        }

    }
}


class TextComparator extends FieldComparator<String> {

    protected final String missingValue;
    protected final String field;


    protected AtomicReader reader;

    private final String[] values;

    public TextComparator(int numHits, String field, String missingValue) {
        this.field = field;
        this.missingValue = missingValue;
        values = new String[numHits];

        System.out.println(numHits);
    }

    @Override
    public int compare(int slot1, int slot2) {
        if(values[slot1] == null) {
            return -1;
        } else if(values[slot2] == null) {
            return 1;
        }
        return values[slot1].compareTo(values[slot2]);
    }

    @Override
    public int compareBottom(int doc) {
        return 1;
    }

    @Override
    public void copy(int slot, int doc) {
        String value = null;
        try {
            value = (String)reader.document(doc, ImmutableSet.<String>of(field)).get(field);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        values[slot] = value;
    }

    @Override
    public FieldComparator<String> setNextReader(AtomicReaderContext context) throws IOException {
        this.reader = context.reader();
        return this;
    }



    @Override
    public void setBottom(final int bottom) {
    }

    @Override
    public String value(int slot) {
        return values[slot];
    }

    @Override
    public int compareDocToValue(int doc, String value) {
        if(value == null) {
            return -1;
        }
        String v = null;
        try {
            v = (String)reader.document(doc, ImmutableSet.<String>of(field)).get(field);
            if(v == null) {
                return 1;
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return value.compareTo(v);
    }
}
