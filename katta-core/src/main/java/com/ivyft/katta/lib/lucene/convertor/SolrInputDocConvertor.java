package com.ivyft.katta.lib.lucene.convertor;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.common.SolrInputDocument;

import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 14-3-24
 * Time: 上午10:26
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public class SolrInputDocConvertor implements DocumentConvertor<SolrInputDocument> {


    public SolrInputDocConvertor() {

    }

    @Override
    public SolrInputDocument convert(Document document, float score) {
        //所有的结果都要存放在result变量中
        SolrInputDocument solrDocument = new SolrInputDocument();
        solrDocument.addField("score", score);

        //i遍历所有的field
        List<IndexableField> fields = document.getFields();
        for (IndexableField field : fields) {
            String name = field.name();
            if(field.stringValue() == null) {
                continue;
            }

            //判断各种数据类型
            if (field.fieldType().docValueType() == FieldInfo.DocValuesType.NUMERIC) {
                solrDocument.addField(name, field.numericValue(), field.boost());
            } else {
                solrDocument.addField(name, field.stringValue(), field.boost());
            }
        }
        return solrDocument;
    }
}
