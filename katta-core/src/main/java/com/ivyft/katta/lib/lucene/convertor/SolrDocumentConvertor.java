package com.ivyft.katta.lib.lucene.convertor;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.common.SolrDocument;

import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-12-19
 * Time: 下午12:38
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class SolrDocumentConvertor implements DocumentConvertor<SolrDocument> {


    public SolrDocumentConvertor() {

    }

    @Override
    public SolrDocument convert(Document document, float score) {
        //所有的结果都要存放在result变量中
        SolrDocument solrDocument = new SolrDocument();
        solrDocument.setField("score", score);

        //i遍历所有的field
        List<IndexableField> fields = document.getFields();
        for (IndexableField field : fields) {
            String name = field.name();
            if(field.stringValue() == null) {
                continue;
            }

            //判断各种数据类型
            if (field.fieldType().docValueType() == FieldInfo.DocValuesType.NUMERIC) {
                solrDocument.addField(name, field.numericValue());
            } else {
                solrDocument.addField(name, field.stringValue());
            }
        }
        return solrDocument;
    }
}
