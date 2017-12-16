package com.ivyft.katta.solr;

import com.google.common.collect.Ordering;
import org.apache.solr.common.SolrDocument;

import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 17/12/16
 * Time: 15:52
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class SolrDocumentSortTest {


    static class StringComparator implements Comparator<SolrDocument> {

        public int asc = 1;


        protected String field;

        public StringComparator(String field) {
            this(field, false);
        }

        public StringComparator(String field, boolean desc) {
            this.field = field;
            this.asc = (desc ? -1 : 1);
        }

        @Override
        public int compare(SolrDocument o1, SolrDocument o2) {
            return asc * ((String)o1.getFieldValue(field)).compareTo((String)o2.getFieldValue(field));
        }
    }


    public static void main(String[] args) {
        //PriorityBlockingQueue<SolrDocument> queue =
        //        new PriorityBlockingQueue(4, new StringComparator("title"));
        Collection<SolrDocument> docs = new ArrayList<>();

        SolrDocument d1 = new SolrDocument();
        d1.addField("title", "aaaa");
        docs.add(d1);

        SolrDocument d2 = new SolrDocument();
        d2.addField("title", "abcd");
        docs.add(d2);

        SolrDocument d3 = new SolrDocument();
        d3.addField("title", "aacd");
        //queue.add(d3);

        SolrDocument d4 = new SolrDocument();
        d4.addField("title", "aaad");
        docs.addAll(Arrays.<SolrDocument>asList(d3, d4));

        List<SolrDocument> title = Ordering.from(new StringComparator("title")).leastOf(docs, 4);
        for (Object o : title) {
            System.out.println(o);
        }

    }
}


