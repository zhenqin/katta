package com.ivyft.katta.lib.writer;


import com.ivyft.katta.codec.Serializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/28
 * Time: 12:14
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class SerialFactory {


    final static Map<String, Serialization> SERIALIZER_MAP = new HashMap<String, Serialization>(2);


    static {
        registry(new JdkSerialization());
    }

    public static void registry(Serialization serialization) {
        if(SERIALIZER_MAP.containsKey(serialization.getContentType())) {
            return;
        }
        SERIALIZER_MAP.put(serialization.getContentType(), serialization);
    }


    public static  <T> Serializer<T> get(String name) {
        Serialization serialization = SERIALIZER_MAP.get(name);
        if(serialization != null) {
            try {
                return serialization.serialize();
            } catch (IOException e) {
                return null;
            }
        }

        return null;
    }
}
