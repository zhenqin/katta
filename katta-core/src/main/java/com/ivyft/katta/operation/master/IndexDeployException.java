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
package com.ivyft.katta.operation.master;


import com.ivyft.katta.protocol.metadata.IndexDeployError;

/**
 *
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
public class IndexDeployException extends Exception {


    /**
     * 序列化
     */
    private static final long serialVersionUID = 1L;

    /**
     * 异常类型
     */
    private final IndexDeployError.ErrorType _errorType;


    /**
     * 构造方法
     * @param errorType
     * @param message
     */
    public IndexDeployException(IndexDeployError.ErrorType errorType, final String message) {
        super(message);
        _errorType = errorType;
    }

    public IndexDeployException(IndexDeployError.ErrorType errorType, final String message, final Throwable cause) {
        super(message, cause);
        _errorType = errorType;
    }

    public IndexDeployError.ErrorType getErrorType() {
        return _errorType;
    }
}
