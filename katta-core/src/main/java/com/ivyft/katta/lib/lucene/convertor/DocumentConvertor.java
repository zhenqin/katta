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


    public T convert(Document document);

}
