/**
 * Copyright 2011 the original author or authors.
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

import com.ivyft.katta.util.NodeConfiguration;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.net.URI;

/**
 *
 * <p>
 *
 * A factory for creating {@link IndexSearcher} on a given shard.
 * Implementations need to have a default constructor.
 *
 * </p>
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
public interface ISeacherFactory {


    /**
     * 初始化 Factory, 获取一些 Node conf
     * @param config Node Conf
     */
    public void init(NodeConfiguration config);



    /**
     * 创建 IndexSearcher
     * @param shardName Index Shard Name
     * @param shardPath Index Shard Path
     * @return 返回根据 Path 创建的 IndexSearcher
     * @throws IOException
     */
    public IndexSearcher createSearcher(String shardName, URI shardPath) throws IOException;


    /**
     *
     * 重新打开索引，Reader reopen，除非真的索引发生了改变，否则调用该方法，损耗搜索性能较大
     *
     * @param indexReader Old Index Reader
     * @param shardName shardName
     * @param shardPath shardPath
     * @return 返回新的 IndexSearcher，否则返回 null
     * @throws IOException
     */
    public IndexSearcher reopenIndex(IndexReader indexReader, String shardName, URI shardPath) throws IOException;
}
