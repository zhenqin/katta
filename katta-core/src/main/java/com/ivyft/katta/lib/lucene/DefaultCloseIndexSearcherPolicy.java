package com.ivyft.katta.lib.lucene;

import com.ivyft.katta.util.NodeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 *
 * <p>
 *     默认的关闭 IndexSearcher 实现, 大于 30 分钟不使用后, 果断 Close
 * </p>
 *
 *
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/2/2
 * Time: 13:15
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class DefaultCloseIndexSearcherPolicy implements CloseIndexSearcherPolicy {


    /**
     * 一分钟
     */
    public final static long MINUTE = 1000L * 60;



    protected int closeSearcherMinutes = 30;


    /**
     * LOG
     */
    private static Logger LOG = LoggerFactory.getLogger(DefaultCloseIndexSearcherPolicy.class);




    @Override
    public void init(NodeConfiguration conf) {
        this.closeSearcherMinutes = conf.getInt("lucene.close.index.search.minutes", 30);
    }




    @Override
    public boolean close(String name, SearcherHandle handle) throws IOException {
        //已关闭
        if(handle.isClosed()) {
            LOG.info(name + " index searcher closed.");
            return true;
        }
        long lastVisited = handle.getLastVisited();
        long now = System.currentTimeMillis();
        int ref = handle.refCount();

        LOG.info("now - lastVisited = " + (now - lastVisited));
        if(ref == 0 && (now - lastVisited >= MINUTE * closeSearcherMinutes)) {
            handle.closeIndexSearcher();
            return true;
        }

        return false;
    }
}
