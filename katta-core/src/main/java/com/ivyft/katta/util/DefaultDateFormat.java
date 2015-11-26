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
package com.ivyft.katta.util;

import org.apache.commons.lang.time.FastDateFormat;


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
public class DefaultDateFormat {

    /**
     * 默认的日期格式化
     */
    public final static FastDateFormat FORMAT = FastDateFormat.getInstance("EEE, d MMM yyyy HH:mm:ss Z");


    /**
     * 返回经过FORMAT格式化的时间
     * @param timeStamp 时间戳
     * @return 返回时间戳被格式化的字符串样式
     */
    public static String longToDateString(final long timeStamp) {
        return FORMAT.format(timeStamp);
    }
}
