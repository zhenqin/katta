package com.ivyft.katta.client.transformer;

import com.ivyft.katta.client.ResultTransformer;
import com.ivyft.katta.lib.lucene.QueryResponse;
import com.ivyft.katta.lib.lucene.ResponseWritable;
import org.apache.solr.common.params.SolrParams;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-12-20
 * Time: 下午1:47
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public class QueryResponseTransformer implements ResultTransformer<QueryResponse> {

    /**
     * 查询结果聚集器
     */
    private final QueryResponse response;


    public QueryResponseTransformer() {
        this(null);
    }

    public QueryResponseTransformer(SolrParams params) {
        response = new QueryResponse(params);
    }

    @Override
    public void transform(Object obj, Collection<String> shards) {
        if(obj instanceof ResponseWritable) {
            QueryResponse resp = ((ResponseWritable)obj).getResponse();
            response.setMaxScore(resp.getMaxScore());
            response.addNumFount(resp.getNumFount());
            response.setQTime(resp.getQTime());
            response.addDocs(resp.getDocs());
        }
    }

    @Override
    public QueryResponse getResult() {
        return response;
    }
}
