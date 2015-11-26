/**
 * Copyright 2009 the original author or authors.
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

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;

/**
 *
 * <p>
 * The public interface to the back end LuceneServer. These are all the
 * methods that the Hadoop RPC will call.
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
public interface ILuceneServer extends VersionedProtocol {


    /**
     * hadoop 序列化
     */
    public final static long versionID = 1L;




    /**
     * 类似Solr的查询
     * @param query
     * @param shards
     * @return
     */
    public ResponseWritable query(QueryWritable query,
                                  String[] shards,
                                  long timeout) throws IOException;


    /**
     *
     * 按照query中的条件进行group。
     *
     * @param query
     * @param shards
     * @return
     */
    public GroupResultWritable group(QueryWritable query,
                                     String[] shards,
                                     long timeout) throws IOException;





    public FacetResultWritable facet(QueryWritable query,
                                     String[] shards,
                                     long timeout) throws IOException;



    /**
     * 对数值区间，时间区间进行Facet。
     * @param query 查询语句
     * @param shards 查询分片
     * @param timeout 超时时间
     * @return 返回Facet的结果
     * @throws IOException
     */
    public FacetResultWritable facetByRange(QueryWritable query,
                                            String[] shards,
                                            long timeout) throws IOException;


    /**
     * Returns all Hits that match the query. This might be significant slower as
     * {@link #search(QueryWritable, String[], long, int)} since we
     * replace count with Integer.MAX_VALUE.
     *
     * @param query      The query to run.
     * @param shardNames A array of shard names to search in.
     * @param timeout    How long the query is allowed to run before getting interrupted
     * @return A list of hits from the search.
     * @throws IOException If the search had a problem reading files.
     */
    public HitsMapWritable search(QueryWritable query,
                                  String[] shardNames,
                                  long timeout) throws IOException;


    /**
     * @param query      The query to run.
     * @param shardNames A array of shard names to search in.
     * @param timeout    How long the query is allowed to run before getting interrupted
     * @param count      The top n high score hits.
     * @return A list of hits from the search.
     * @throws IOException If the search had a problem reading files.
     */
    public HitsMapWritable search(QueryWritable query,
                                  String[] shardNames,
                                  long timeout,
                                  int count)
            throws IOException;


    /**
     * Returns only the requested fields of a lucene document.  The fields are returned
     * as a map.
     *
     * @param shards The shards to ask for the document.
     * @param docId  The document that is desired.
     * @param fields The fields to return.
     * @return details of the document
     * @throws IOException
     */
    public MapWritable getDetail(String[] shards,
                                 int docId,
                                 String[] fields) throws IOException;

    /**
     * Returns the lucene document. Each field:value tuple of the lucene document
     * is inserted into the returned map. In most cases
     * {@link #getDetail(String[], int, String[])} would be a better choice for
     * performance reasons.
     *
     * @param shards The shards to ask for the document.
     * @param docId  The document that is desired.
     * @return details of the document
     * @throws IOException
     */
    public MapWritable getDetail(String[] shards,
                                 int docId) throws IOException;



    /**
     * Returns the lucene document. Each field:value tuple of the lucene document
     * is inserted into the returned map. In most cases
     * {@link #getDetail(String[], int, String[])} would be a better choice for
     * performance reasons.
     *
     * @param shards The shards to ask for the document.
     * @param docIds  The documents that is desired.
     * @return details of the document
     * @throws IOException
     */
    public ArrayMapWritable getDetails(String[] shards,
                                       int[] docIds) throws IOException;

    /**
     * Returns the number of documents that match the given query. This the
     * fastest way in case you just need the number of documents. Note that the
     * number of matching documents is also included in HitsMapWritable.
     *
     * @param query
     * @param shards
     * @param timeout How long the query is allowed to run before getting interrupted
     * @return number of documents
     * @throws IOException
     */
    public int count(QueryWritable query,
                     String[] shards,
                     long timeout) throws IOException;

}
