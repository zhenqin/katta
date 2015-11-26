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
package com.ivyft.katta.operation;

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
public class OperationId implements Serializable {


    /**
     * 序列化
     */
    private static final long serialVersionUID = 1L;


    /**
     * 事件发布到的Node
     */
    private final String nodeName;

    /**
     * 事件ID
     */
    private final String elementName;


    /**
     * 构造方法
     * @param nodeName
     * @param elementName
     */
    public OperationId(String nodeName, String elementName) {
        this.nodeName = nodeName;
        this.elementName = elementName;
    }


    public String getNodeName() {
        return nodeName;
    }

    public String getElementName() {
        return elementName;
    }

    @Override
    public String toString() {
        return nodeName + "-" + elementName;
    }

}
