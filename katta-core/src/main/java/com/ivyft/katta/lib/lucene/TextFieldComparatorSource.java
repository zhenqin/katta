package com.ivyft.katta.lib.lucene;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;

import java.io.IOException;

/**
 *
 *
 * <p>
 *     该类是 Lucene 分词类型（大文本）字段的排序实现，默认按照字典顺序升序排列。
 *
 * </p>
 *
 *
 * <p>
 *     请注意： 该类性能并不高，有潜在的内存溢出风险。
 * </p>
 *
 *
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/3/24
 * Time: 17:31
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */


public class TextFieldComparatorSource extends FieldComparatorSource {

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
        return new TextComparator(numHits, fieldname);
    }



    public class TextComparator extends FieldComparator<String> {

        protected final String field;


        protected AtomicReader reader;

        private final String[] values;

        public TextComparator(int numHits, String field) {
            this.field = field;
            values = new String[numHits];
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

}

