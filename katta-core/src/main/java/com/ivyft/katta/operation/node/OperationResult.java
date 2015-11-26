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

import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;


/**
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
public class OperationResult implements Serializable {

    /**
     * 序列化
     */
    private static final long serialVersionUID = 1L;

    /**
     * Node Name
     */
    private final String nodeName;

    /**
     * 可能发生的异常
     */
    private final Exception unhandledException;


    /**
     * 构造方法
     * @param nodeName
     */
    public OperationResult(String nodeName) {
        this(nodeName, null);
    }

    public OperationResult(String nodeName, Exception unhandledException) {
        this.nodeName = nodeName;
        this.unhandledException = unhandledException;
    }

    public String getNodeName() {
        return nodeName;
    }

    public Exception getUnhandledException() {
        return unhandledException;
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this).
                append("nodeName", nodeName).
                append("unhandledException", unhandledException).
                toString();
    }
}
