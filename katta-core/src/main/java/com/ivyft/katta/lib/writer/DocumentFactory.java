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
     * 反序列化
     *
     * @param context 序列化上下文
     * @param buffer 一个数据 byte
     * @return 返回 byte 可以反序列化的对象 List
     */
    public Collection<T> deserial(SerdeContext context, ByteBuffer buffer);


    /**
     * 从 List 对象中返回 Document
     * @param context 序列化
     * @param list 对象 List
     * @return 返回 Lucene Document
     */
    public Collection<Document> get(SerdeContext context, Collection<T> list);
}
