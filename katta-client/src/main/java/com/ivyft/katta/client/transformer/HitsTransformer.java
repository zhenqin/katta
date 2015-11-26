package com.ivyft.katta.client.transformer;

import com.ivyft.katta.client.ResultTransformer;
import com.ivyft.katta.lib.lucene.Hits;
import com.ivyft.katta.lib.lucene.HitsMapWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-12-19
 * Time: 下午6:11
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class HitsTransformer implements ResultTransformer<Hits> {


    private Hits hits = new Hits();


    private static Logger log = LoggerFactory.getLogger(HitsTransformer.class);

    public HitsTransformer() {

    }


    @Override
    public void transform(Object obj, Collection<String> shards) {
        if(obj instanceof HitsMapWritable) {
            HitsMapWritable add = (HitsMapWritable)obj;

            hits.addHits(add.getHitList());
            hits.addTotalHits(add.getTotalHits());
//            if(!add.getMissingShards().isEmpty()) {
//                log.warn("incomplete result - missing shard-results: " + hits.getMissingShards() + ", "
//                        + add.getShardCoverage());
//                hits.addMissingShards(add.getMissingShards());
//            }
        }
    }

    @Override
    public Hits getResult() {
        return hits;
    }
}
