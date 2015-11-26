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
package com.ivyft.katta.protocol.metadata;


import com.ivyft.katta.util.DefaultDateFormat;

import java.io.Serializable;


/**
 *
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
public class NodeMetaData implements Serializable {

    /**
     * 序列化表格
     */
    private static final long serialVersionUID = 1L;


    /**
     * 机器名+RPC port
     */
    private String name;


    /**
     * 每条查询使用的时间
     */
    private float queriesPerMinute = 0f;


    /**
     * Node启动时间
     */
    private long startTimeStamp = System.currentTimeMillis();


    /**
     * with node execution
     */
    public NodeMetaData() {
        // for serialization
    }


    /**
     * 构造方法
     * @param name
     */
    public NodeMetaData(final String name) {
        this.name = name;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public long getStartTimeStamp() {
        return startTimeStamp;
    }

    public void setStartTimeStamp(long startTimeStamp) {
        this.startTimeStamp = startTimeStamp;
    }

    public String startTimeAsDate() {
        return DefaultDateFormat.longToDateString(startTimeStamp);
    }

    public float getQueriesPerMinute() {
        return queriesPerMinute;
    }

    public void setQueriesPerMinute(float queriesPerMinute) {
        this.queriesPerMinute = queriesPerMinute;
    }

    @Override
    public String toString() {
        return getName() + "\t:\t" + startTimeAsDate() + "";
    }

}
