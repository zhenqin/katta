package com.ivyft.katta.lib.lucene.convertor;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-12-19
 * Time: 下午1:03
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class DefaultDocumentConvertor  implements DocumentConvertor<Document> {

    public DefaultDocumentConvertor() {

    }

    @Override
    public Document convert(Document document, float score) {
        document.add(new FloatField("score", score, Field.Store.NO));
        return document;
    }
}
