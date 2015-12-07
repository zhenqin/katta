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
package com.ivyft.katta.client;

import com.ivyft.katta.protocol.metadata.IndexMetaData;

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
 *
 * @author ZhenQin
 *
 */
public interface IDeployClient {


    /**
     * 创建一个索引集
     * @param indexName 索引集名称
     * @param indexPath 索引集路径
     * @return 返回创建索引句柄
     *
     */
    public IIndexDeployFuture createIndex(String indexName,
                                          String indexPath,
                                          int shardNum,
                                          int shardStep);



    /**
     * 部署一份索引.
     * @param name 索引名称
     * @param path 索引Path
     * @param replicationLevel 复制级别
     * @return 返回不是的Future
     */
    IIndexDeployFuture addIndex(String name,
                                String path,
                                String collectionName,
                                int replicationLevel);


    /**
     * 给索引添加一份Shard
     * @param name IndexName
     * @param path Shard path
     * @return 返回执行结果
     */
    IIndexDeployFuture addShard(String name, String path);


    /**
     * 移除shard
     * @param name 名称
     */
    void removeIndex(String name);


    /**
     * 给Index删除一份索引path
     * @param name Index name
     * @param path Shard name
     *
     */
    void removeShard(String name, String path);


    /**
     * 验证是否存在部署的shardName
     * @param name shardName
     * @return true已经部署过
     */
    boolean existsIndex(String name);


    /**
     * 返回索引信息, 大小等
     * @param name  shardName
     * @return 索引原信息
     */
    IndexMetaData getIndexMetaData(String name);


    /**
     * 返回所有部署的索引
     * @return 所有部署的索引
     */
    List<String> getIndices();

}
