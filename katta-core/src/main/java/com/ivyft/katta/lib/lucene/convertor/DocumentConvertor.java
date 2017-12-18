package com.ivyft.katta.lib.lucene.convertor;

import org.apache.lucene.document.Document;

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
public interface DocumentConvertor<T> {


    /**
     * 转换 Lucene Document 为其它形式
     * @param document LuceneDocument
     * @param score 分数
     * @return
     */
    public T convert(Document document, float score);

}
