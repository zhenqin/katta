package com.ivyft.katta.lib.writer;

import com.ivyft.katta.codec.Serializer;
import org.apache.lucene.document.Document;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/28
 * Time: 12:18
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class JdkSerialFactory<T> implements DocumentFactory<T> {



    public Collection<T> deserial(SerdeContext context, ByteBuffer buffer) {
        Serializer<Object> serializer = SerialFactory.get(context.getSerdeName());
        T deserialize = (T) serializer.deserialize(buffer.array());
        return Arrays.asList(deserialize);
    }

    @Override
    public Collection<Document> get(T obj) {
        //集合,List,Set
        if(obj instanceof Collection) {
            return null;
        }

        //数组
        if(obj instanceof Array) {

        }

        //单个对象
        List<Document> docs = new ArrayList<Document>();
        return docs;
    }
}
