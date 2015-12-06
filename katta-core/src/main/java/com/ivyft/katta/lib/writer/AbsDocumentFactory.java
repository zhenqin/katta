package com.ivyft.katta.lib.writer;

import com.ivyft.katta.codec.Serializer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/6
 * Time: 14:11
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public abstract class AbsDocumentFactory<T> implements DocumentFactory<T> {


    protected Map<String, Serializer> serializerMap = new HashMap<String, Serializer>(3);


    @Override
    public List<T> deserial(SerdeContext context, ByteBuffer buffer) {
        Serializer<T> serializer = serializerMap.get(context.getSerClass());
        if(serializer == null){
            try {
                Class<? extends Serialization> serializationClass =
                        (Class<? extends Serialization>) Class.forName(context.getSerClass());

                serializer = serializationClass.newInstance().serialize();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        T obj = serializer.deserialize(buffer.array());
        if(obj instanceof List) {
            return (List)obj;
        }
        return Arrays.asList(obj);
    }
}
