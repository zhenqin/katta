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

import com.google.common.base.Objects;
import com.ivyft.katta.util.One2ManyListMap;

import java.io.Serializable;
import java.util.List;


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
public class IndexDeployError implements Serializable {

    private static final long serialVersionUID = 1L;

    public static enum ErrorType {
        NO_NODES_AVAILIBLE, INDEX_NOT_ACCESSIBLE, SHARDS_NOT_DEPLOYABLE, UNKNOWN;
    }

    private final String _indexName;
    private final ErrorType _errorType;
    private final One2ManyListMap<String, Exception> _shard2ExceptionsMap = new One2ManyListMap<String, Exception>();
    private Exception _exception;

    public IndexDeployError(String indexName, ErrorType errorType) {
        _indexName = indexName;
        _errorType = errorType;
    }

    public String getIndexName() {
        return _indexName;
    }

    public ErrorType getErrorType() {
        return _errorType;
    }

    public void setException(Exception exception) {
        _exception = exception;
    }

    public Exception getException() {
        return _exception;
    }

    public void addShardError(String shardName, Exception exception) {
        _shard2ExceptionsMap.add(shardName, exception);
    }

    public List<Exception> getShardErrors(String shardName) {
        return _shard2ExceptionsMap.getValues(shardName);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(_indexName).addValue(_errorType).addValue(_exception).toString();
    }
}
