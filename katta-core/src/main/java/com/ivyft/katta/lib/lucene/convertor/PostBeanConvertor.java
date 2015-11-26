package com.ivyft.katta.lib.lucene.convertor;

import org.apache.lucene.document.Document;

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
public class PostBeanConvertor implements DocumentConvertor<Document> {

    public PostBeanConvertor() {
        
    }

    @Override
    public Document convert(Document document) {
        return document;
    }
}
