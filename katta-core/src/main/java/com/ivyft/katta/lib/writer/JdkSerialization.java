package com.ivyft.katta.lib.writer;

import com.ivyft.katta.codec.Serializer;
import com.ivyft.katta.codec.jdkserializer.JdkSerializer;

import java.io.IOException;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/28
 * Time: 12:22
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class JdkSerialization implements Serialization {

    public JdkSerialization() {

    }

    public byte getContentTypeId() {
        return 10;
    }

    public String getContentType() {
        return "JDK";
    }

    public Serializer serialize()
            throws IOException {
        return new JdkSerializer();
    }
}
