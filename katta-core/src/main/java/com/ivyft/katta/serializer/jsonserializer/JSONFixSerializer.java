/*
 * Copyright 2010-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ivyft.katta.serializer.jsonserializer;

import com.alibaba.fastjson.JSON;
import com.google.common.primitives.Ints;
import com.ivyft.katta.serializer.strserializer.StringSerializer;
import org.I0Itec.zkclient.serialize.ZkSerializer;


/**
 * Java Serialization Redis strserializer.
 * Delegates to the default (Java based) strserializer in Spring 3.
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-13
 * Time: 上午8:58
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class JSONFixSerializer<T> implements ZkSerializer {

    private StringSerializer serializer = new StringSerializer();

    public JSONFixSerializer() {

    }


    @Override
    public byte[] serialize(Object o) {
        String clazz = o.getClass().getName();
        byte[] classString = clazz.getBytes(serializer.getCharset());

        int length = classString.length;
        byte[] head = Ints.toByteArray(length);

        byte[] body = serializer.serialize(JSON.toJSONString(o));
        byte[] bytes = new byte[head.length + length + body.length];
        System.arraycopy(head, 0, bytes, 0, head.length);
        System.arraycopy(classString, 0, bytes, head.length, classString.length);
        System.arraycopy(body, 0, bytes, head.length + length, body.length);
        return bytes;
    }

    @Override
    public T deserialize(byte[] bytes) {
        byte[] head = new byte[4];
        System.arraycopy(bytes, 0, head, 0, head.length);

        int length = Ints.fromBytes(bytes[0], bytes[1], bytes[2], bytes[3]);

        String classString = new String(bytes, 4, length);

        try {
            return (T)JSON.toJavaObject(
                    JSON.parseObject(serializer.deserialize(bytes, 4 + length, bytes.length - 4 - length)),
                            Class.forName(classString));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }
}