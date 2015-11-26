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
package com.ivyft.katta.codec.jsonserializer;

import com.alibaba.fastjson.JSON;
import com.ivyft.katta.codec.Serializer;
import com.ivyft.katta.codec.strserializer.StringSerializer;


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
public class JSONSerializer<T> implements Serializer<T> {

    private StringSerializer serializer = new StringSerializer();


    private Class<? extends T> reverseClazz;


    public JSONSerializer(Class<? extends T> clazz) {
        this.reverseClazz = clazz;
    }


    @Override
    public byte[] serialize(Object o) {
        return serializer.serialize(JSON.toJSONString(o));
    }

    @Override
    public T deserialize(byte[] bytes) {
        return JSON.toJavaObject(
                JSON.parseObject(serializer.deserialize(bytes)), reverseClazz);
    }


    public Class<? extends T> getReverseClazz() {
        return reverseClazz;
    }
}