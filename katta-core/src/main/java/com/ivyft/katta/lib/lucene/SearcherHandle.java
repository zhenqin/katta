package com.ivyft.katta.lib.lucene;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
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
     * 创建该索引的 SearcherFactory
     */
    private final ISeacherFactory seacherFactory;


    /**
     * 索引存储的目录
     */
    private final URI shardDir;


    /**
     * 索引名称
     */
    private final String collectionName;


    /**
     * 索引名称, Shard Name
     */
    private final String shardName;


    /**
     * 当前索引是否发生改变
     */
    private boolean change = false;


    /**
     *
     * 当前所有是否已经关闭
     *
     */
    private final AtomicBoolean closed = new AtomicBoolean(true);


    /**
     * 最后访问索引的时间
     */
    private long lastVisited = 0;



    /**
     * Shard的Lucene IndexSearcher
     */
    private volatile IndexSearcher indexSearcher;



    /**
     * 引用计数器
     */
    private final AtomicInteger _refCount = new AtomicInteger(0);


    /**
     * Log
     */
    private static Logger LOG = LoggerFactory.getLogger(SearcherHandle.class);


    /**
     *
     * @param seacherFactory
     * @param shardName
     * @param shardDir
     */
    public SearcherHandle(ISeacherFactory seacherFactory, String collectionName, String shardName, URI shardDir) {
        this.seacherFactory = seacherFactory;
        this.shardDir = shardDir;
        this.shardName = shardName;
        this.collectionName = collectionName;
    }


    /**
     *
     * 初始化索引
     *
     * @return 初始化成功, 返回. 也可使用 context.this.IndexSearcher
     */
    protected IndexSearcher initIndexSearcher() {
        try {
            this.indexSearcher = this.seacherFactory.createSearcher(shardName, shardDir);
            LOG.info("createSearcher, shardDir: " + shardDir);
            this.lastVisited = System.currentTimeMillis();
            closed.set(false);
            change = false;
            return this.indexSearcher;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }





    /**
     *
     * 初始化索引
     *
     * @return 初始化成功, 返回. 也可使用 context.this.IndexSearcher
     */
    protected synchronized IndexSearcher reopenIndexSearcher() {
        try {
            IndexSearcher newSearcher = this.seacherFactory.reopenIndex(this.indexSearcher.getIndexReader(), shardName, shardDir);
            if(newSearcher != null) {
                this.indexSearcher = newSearcher;
            }
            this.lastVisited = System.currentTimeMillis();
            closed.set(false);
            change = false;
            return this.indexSearcher;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    /**
     *
     * Returns the IndexSearcher and increments the usage count.
     * finishSearcher() must be called once after each call to getSearcher().
     *
     * @return the searcher
     */
    public synchronized IndexSearcher getSearcher() {
        //_refCount 如果是0, 则表示长时间没有访问而关闭的, 如果是-1, 则代表 Node 将要 shutdown
        if(this.indexSearcher == null) {
            if(_refCount.get() >= 0) {
                initIndexSearcher();
            } else {
                return null;
            }
        }

        //当索引有变化，重新打开索引
        if(change) {
            change = false;
            try {
                reopenIndexSearcher();
            } catch (Exception e) {
                LOG.warn(e.getMessage());
            }
        }

        _refCount.incrementAndGet();
        this.lastVisited = System.currentTimeMillis();
        return this.indexSearcher;
    }


    /**
     * 索引已经发生改变
     */
    public void indexChanged() {
        change = true;
    }


    /**
     *
     * Decrements the searcher usage count.
     *
     */
    public synchronized void finishSearcher() {
        _refCount.decrementAndGet();
    }



    public boolean isClosed() {
        return closed.get();
    }



    public int refCount() {
        return _refCount.get();
    }



    public long getLastVisited() {
        return lastVisited;
    }


    public ISeacherFactory getSeacherFactory() {
        return seacherFactory;
    }

    public URI getShardDir() {
        return shardDir;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getShardName() {
        return shardName;
    }



    /**
     *
     * @param policy
     */
    public synchronized void closeWithPolicy(String name, CloseIndexSearcherPolicy policy)
            throws IOException {
        boolean close = policy.close(name, this);
        closed.set(close);
        LOG.debug("close " + name + " index searcher result: " + close);
        //已经关闭了 IndexSearcher
        if(close) {
            _refCount.set(0);
            this.indexSearcher = null;
        }
    }


    /**
     * Policy Close IndexSearcher
     *
     * @throws IOException
     */
    public synchronized void closeIndexSearcher() throws IOException {
        this.indexSearcher.getIndexReader().close();
        closed.set(true);
        LOG.warn(getCollectionName() + "'s shard " + shardName + " searcher closed.");
    }



    /**
     *
     *
     * Spins until the searcher is no longer in use, then closes it.
     *
     * @param name name只是用户打印日志用
     * @throws IOException on IndexSearcher close failure
     *
     */
    public synchronized void closeSearcher(String name) throws IOException {
        int times = 0;
        while (true) {
            if(indexSearcher == null) {
                return;
            }
            if (_refCount.get() == 0) {
                try {
                    closeIndexSearcher();
                    LOG.info("closed " + name + " IndexSearcher.");
                    closed.set(true);
                } catch (Exception e) {
                    LOG.error(ExceptionUtils.getMessage(e));
                }
                this.indexSearcher = null;
                _refCount.set(-1);
                return;
            } else if (times >= 5) {
                try {
                    closeIndexSearcher();
                    closed.set(true);
                    LOG.info("closed " + name + " IndexSearcher, refCount: " + _refCount.get());
                } catch (Exception e) {
                    LOG.error(ExceptionUtils.getMessage(e));
                }

                this.indexSearcher = null;
                _refCount.set(-1);
                return;
            } else {
                times++;
                LOG.info("close " + name + " IndexSearcher, refCount: " +
                        _refCount.get() + " retry " + (5 - times));
            }

            try {
                Thread.sleep(LuceneServer.INDEX_HANDLE_CLOSE_SLEEP_TIME);
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        }

    }
}
