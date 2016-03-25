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



    public Collection<T> deserial(SerdeContext context, ByteBuffer buffer);




    public Collection<Document> get(SerdeContext context, Collection<T> list);
}
