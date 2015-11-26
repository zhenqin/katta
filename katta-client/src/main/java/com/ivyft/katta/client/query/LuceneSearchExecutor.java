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
package com.ivyft.katta.client.query;

import com.ivyft.katta.client.ILuceneClient;
import com.ivyft.katta.client.LuceneClient;
import com.ivyft.katta.client.mapfile.AbstractQueryExecutor;
import com.ivyft.katta.node.NodeContext;
import com.ivyft.katta.util.ZkConfiguration;
import org.apache.solr.client.solrj.SolrQuery;


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
public class LuceneSearchExecutor extends AbstractQueryExecutor {

    private final int _count;
    private ILuceneClient _client;
    private final ZkConfiguration _zkConfOfTargetCluster;

    public LuceneSearchExecutor(String[] indices, String[] queries, ZkConfiguration zkConfOfTargetCluster, int count) {
        super(indices, queries);
        _zkConfOfTargetCluster = zkConfOfTargetCluster;
        _count = count;
    }

    @Override
    public void init(NodeContext nodeContext) throws Exception {
        _client = new LuceneClient(_zkConfOfTargetCluster);
    }

    @Override
    public void close(NodeContext nodeContext) throws Exception {
        _client.close();
        _client = null;
    }

    @Override
    public void execute(NodeContext nodeContext, String queryString) throws Exception {
        final SolrQuery query = new SolrQuery(queryString);
        query.setRows(_count);
        _client.search(query, _indices);
    }

}
