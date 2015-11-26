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
public class MasterMetaData implements Serializable {

    private static final long serialVersionUID = 1L;


    /**
     * Master Name
     */
    private String masterName;


    /**
     * 启动时间
     */
    private long startTime;


    public MasterMetaData() {
    }

    public MasterMetaData(String masterName, long startTime) {
        this.masterName = masterName;
        this.startTime = startTime;
    }

    public String getStartTimeAsString() {
        return DefaultDateFormat.longToDateString(this.startTime);
    }


    public String getMasterName() {
        return masterName;
    }

    public void setMasterName(String masterName) {
        this.masterName = masterName;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    @Override
    public String toString() {
        return getMasterName() + ":" + getStartTime();
    }
}
