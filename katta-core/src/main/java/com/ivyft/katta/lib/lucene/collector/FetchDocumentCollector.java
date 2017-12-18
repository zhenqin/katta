package com.ivyft.katta.lib.lucene.collector;

import com.ivyft.katta.lib.lucene.convertor.DocumentConvertor;
import com.ivyft.katta.lib.lucene.convertor.SolrDocumentConvertor;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TotalHitCountCollector;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-12-19
 * Time: 上午10:59
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class FetchDocumentCollector extends TotalHitCountCollector {


    /**
     * Collector Reader Context
     */
    protected AtomicReaderContext context;


    /**
     * 需要读出的Field
     */
    protected Set<String> fields;


    /**
     * 稳定集合
     */
    protected List<Object> docs = new LinkedList<Object>();


    /**
     * 读取数量
     */
    protected int limit = 100;


    /**
     * 开始位移
     */
    protected int offset = 0;


    /**
     * 该文档的得分
     */
    protected float score = 0.0f;


    /**
     * Lucene Document Convertor，默认转换成Solr Input Document
     */
    private DocumentConvertor convertor = new SolrDocumentConvertor();


    /**
     * 构造方法
     * @param fields 字段限制
     * @param limit 限制数量
     * @param offset 起始位移
     */
    public FetchDocumentCollector(Set<String> fields, int limit, int offset) {
        this.fields = fields;
        this.limit = limit;
        this.offset = offset;
    }


    @Override
    public void setNextReader(AtomicReaderContext context) {
        super.setNextReader(context);
        this.context = context;
    }


    @Override
    public void setScorer(Scorer scorer) {
        try {
            this.score = scorer.score();
        } catch (IOException e) {
            this.score = 0.1f;
        }
    }

    @Override
    public void collect(int doc) {
        super.collect(doc);
        if(docs.size() < limit && getTotalHits() > offset) {
            if(fields != null) {
                try {
                    docs.add(convertor.convert(context.reader().document(doc, fields), this.score));
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            } else {
                try {
                    docs.add(convertor.convert(context.reader().document(doc), this.score));
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        }
    }


    public Set<String> getFields() {
        return fields;
    }


    public <Type> List<Type> getDocs() {
        return (List<Type>)docs;
    }


    public DocumentConvertor getConvertor() {
        return convertor;
    }

    public void setConvertor(DocumentConvertor convertor) {
        this.convertor = convertor;
    }

}
