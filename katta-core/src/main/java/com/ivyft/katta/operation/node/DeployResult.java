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
package com.ivyft.katta.operation.node;

import java.util.HashMap;
import java.util.Map;


/**
 *
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-19
 * Time: 上午8:59
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class DeployResult extends OperationResult {


    /**
     * 序列化
     */
    private static final long serialVersionUID = 1L;



    private Map<String, Exception> exceptionByShard = new HashMap<String, Exception>(3);



    private Map<String, Map<String, String>> metadataMapByShard = new HashMap<String, Map<String, String>>(3);


    /**
     * constructor
     * @param nodeName
     */
    public DeployResult(String nodeName) {
        super(nodeName);
    }

    public void addShardException(String shardName, Exception exception) {
        exceptionByShard.put(shardName, exception);
    }

    public void addShardMetaDataMap(String shardName, Map<String, String> shardMetaData) {
        metadataMapByShard.put(shardName, shardMetaData);
    }

    public Map<String, Exception> getShardExceptions() {
        return exceptionByShard;
    }

    public Map<String, Map<String, String>> getShardMetaDataMaps() {
        return metadataMapByShard;
    }

}
