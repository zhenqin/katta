package com.ivyft.katta.lib.writer;

import org.apache.lucene.document.Document;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/28
 * Time: 12:09
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public interface DocumentFactory<T> {


    /**
     * 从 List 对象中返回 Document
     * @param obj 对象
     * @return 返回 Lucene Document
     */
    public Collection<Document> get(T obj);
}
