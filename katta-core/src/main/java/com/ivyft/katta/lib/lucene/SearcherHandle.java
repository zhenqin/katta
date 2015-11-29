package com.ivyft.katta.lib.lucene;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.lucene.search.IndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * <p>
 *
 * Holds an IndexSearcher and maintains the current number of threads using
 * it. For every call to getSearcher(), finishSearcher() must be called
 * exactly one time. finally blocks are a good idea.
 *
 * </p>
 *
 * <p>
 *     该类是一个IndexSearcher的锁服务和计数器。 用来计算当前IndexSearcher被引用多少次
 * </p>
 *
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-19
 * Time: 上午9:14
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class SearcherHandle {

    /**
     * Shard的Lucene IndexSearcher
     */
    private volatile IndexSearcher indexSearcher;

    /**
     * 一个同步的锁
     */
    private final Object _lock = new Object();


    /**
     * 引用计数器
     */
    private final AtomicInteger _refCount = new AtomicInteger(0);


    /**
     * Log
     */
    private static Logger log = LoggerFactory.getLogger(SearcherHandle.class);


    /**
     *
     * @param indexSearcher
     */
    public SearcherHandle(IndexSearcher indexSearcher) {
        this.indexSearcher = indexSearcher;
    }

    /**
     * Returns the IndexSearcher and increments the usage count.
     * finishSearcher() must be called once after each call to getSearcher().
     *
     * @return the searcher
     */
    public IndexSearcher getSearcher() {
        synchronized (_lock) {
            if (_refCount.get() < 0) {
                return null;
            }
            _refCount.incrementAndGet();
        }
        return this.indexSearcher;
    }

    /**
     * Decrements the searcher usage count.
     */
    public void finishSearcher() {
        synchronized (_lock) {
            _refCount.decrementAndGet();
        }
    }

    /**
     * Spins until the searcher is no longer in use, then closes it.
     * @param name name只是用户打印日志用
     * @throws IOException on IndexSearcher close failure
     */
    public void closeSearcher(String name) throws IOException {
        int times = 0;
        while (true) {
            synchronized (_lock) {
                if (_refCount.get() == 0) {
                    try {
                        this.indexSearcher.getIndexReader().close();
                        log.info("closed " + name + " IndexSearcher.");
                    } catch (Exception e) {
                        log.error(ExceptionUtils.getFullStackTrace(e));
                    }
                    this.indexSearcher = null;
                    _refCount.set(-1);
                    return;
                } else if(times >= 5) {
                    try {
                        this.indexSearcher.getIndexReader().close();
                        log.info("closed " + name + " IndexSearcher, refCount: " + _refCount.get());
                    } catch (Exception e) {
                        log.error(ExceptionUtils.getFullStackTrace(e));
                    }

                    this.indexSearcher = null;
                    _refCount.set(-1);
                    return;
                } else {
                    times++;
                    log.info("close " + name + " IndexSearcher, refCount: " +
                            _refCount.get() + " retry " + (5 - times));
                }
            }
            try {
                Thread.sleep(LuceneServer.INDEX_HANDLE_CLOSE_SLEEP_TIME);
            } catch (InterruptedException e) {
            }
        }
    }
}
