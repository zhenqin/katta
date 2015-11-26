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
package com.ivyft.katta.codec.jdkserializer;

import com.ivyft.katta.codec.Serializer;
import org.apache.commons.lang.ArrayUtils;

import java.io.*;

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
 * @author Mark Pollack
 * @author Costin Leau
 */
public class JdkSerializer<T extends Serializable> implements Serializer<T> {


    public JdkSerializer() {
    }

    public T deserialize(byte[] bytes) {
        if (ArrayUtils.isEmpty(bytes)) {
            return null;
        }
        try {
            ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));
            return (T)in.readObject();
        } catch (Exception ex) {
            throw new IllegalArgumentException("Cannot deserialize", ex);
        }
    }


    public byte[] serialize(T object) {
        if (object == null) {
            return new byte[0];
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream outputStream = new ObjectOutputStream(out);
            outputStream.writeObject(object);
            return out.toByteArray();
        } catch (Exception ex) {
            throw new IllegalArgumentException("Cannot serialize", ex);
        }
    }
}