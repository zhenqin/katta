/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ivyft.katta.lib.lucene;

import com.ivyft.katta.serializer.jdkserializer.JdkSerializer;
import org.apache.hadoop.io.Writable;
import org.apache.solr.client.solrj.SolrQuery;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;


/**
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
public class QueryWritable implements Writable, Serializable {


    /**
     * 序列化
     */
    private final static long serialVersionUID = 1L;

    /**
     * Solr Queri
     */
    private SolrQuery query;


    /**
     * constructor
     */
    public QueryWritable() {
        // for serialization
    }

    public QueryWritable(SolrQuery query) {
        this.query = query;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        int readInt = input.readInt();
        byte[] bs = new byte[readInt];
        input.readFully(bs);

        JdkSerializer serializer = new JdkSerializer();
        query = (SolrQuery)serializer.deserialize(bs);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        JdkSerializer serializer = new JdkSerializer();
        byte[] byteArray = serializer.serialize(query);
        output.writeInt(byteArray.length);
        output.write(byteArray);
    }


    public void setQuery(SolrQuery query) {
        this.query = query;
    }

    public SolrQuery getQuery() {
        return query;
    }

    @Override
    public boolean equals(Object obj) {
        if (getClass() != obj.getClass()) {
            return false;
        }
        SolrQuery other = ((QueryWritable) obj).getQuery();
        return query.equals(other);
    }

    @Override
    public String toString() {
        return query != null ? query.toString() : "null";
    }

}
