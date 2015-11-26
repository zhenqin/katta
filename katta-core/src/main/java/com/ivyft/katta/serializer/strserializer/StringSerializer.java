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
package com.ivyft.katta.serializer.strserializer;


import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.lang.ArrayUtils;

import java.nio.charset.Charset;

/**
 * Java Serialization Redis strserializer.
 * Delegates to the default (Java based) strserializer in Spring 3.
 *
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
public class StringSerializer implements ZkSerializer {


    private Charset charset = Charset.forName("UTF-8");


    public StringSerializer() {
    }


    public StringSerializer(Charset charset) {
        this.charset = charset;
    }


    public StringSerializer(String charset) {
        this.charset = Charset.forName(charset);
    }


    public String deserialize(byte[] bytes, int offset, int length) {
        if (ArrayUtils.isEmpty(bytes)) {
            return null;
        }
        try {
            return new String(bytes, offset, length, charset);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Cannot deserialize", ex);
        }
    }

    public String deserialize(byte[] bytes) {
        if (ArrayUtils.isEmpty(bytes)) {
            return null;
        }
        try {
            return new String(bytes, charset);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Cannot deserialize", ex);
        }
    }


    public byte[] serialize(Object object) {
        if (object == null) {
            return new byte[0];
        }
        if (!(object instanceof String)) {
            throw new IllegalArgumentException("unkown type, object " + object);
        }
        try {
            return object.toString().getBytes(charset);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Cannot serialize", ex);
        }
    }

    public Charset getCharset() {
        return charset;
    }
}