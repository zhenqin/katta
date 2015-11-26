package com.ivyft.katta.client.transformer;

import com.ivyft.katta.client.ResultTransformer;
import com.ivyft.katta.lib.lucene.QueryResponse;
import com.ivyft.katta.lib.lucene.ResponseWritable;

import java.util.Collection;

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

    private QueryResponse response = new QueryResponse();

    public QueryResponseTransformer() {

    }

    @Override
    public void transform(Object obj, Collection<String> shards) {
        if(obj instanceof ResponseWritable) {
            QueryResponse resp = ((ResponseWritable)obj).getResponse();
            response.addDocs(resp.getDocs());
            response.setMaxScore(resp.getMaxScore());
            response.addNumFount(resp.getNumFount());
            response.setQTime(resp.getQTime());
        }
    }

    @Override
    public QueryResponse getResult() {
        return response;
    }
}
