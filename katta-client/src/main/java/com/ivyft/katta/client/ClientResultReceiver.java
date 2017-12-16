package com.ivyft.katta.client;

import com.ivyft.katta.util.KattaException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-12-20
 * Time: 下午3:58
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public class ClientResultReceiver<T> implements IResultReceiver<T> {


    /**
     * 断定当前结果是否可用
     */
    private boolean closed = false;


    /**
     * 一次搜索启动开始时间
     */
    private final long startTime = System.currentTimeMillis();


    /**
     * 计算发生了多少次RPC,次数够了立即close
     */
    private final int nodeCount;


    /**
     * 搜索结束后关闭, 清理工作
     */
    private final IClosedListener closedListener;



    /**
     * 搜索前所有的shard
     */
    private final Set<String> allShards;

    /**
     * 目前已经完成的shard, 包括搜索异常
     */
    private final Set<String> seenShards = new HashSet<String>();


    /**
     * 异常列表
     */
    private final Set<Entry<T>> entries = new HashSet<Entry<T>>();

    /**
     * 每个节点发生异常的异常保存列表
     */
    private final Collection<Throwable> errors = new LinkedList<Throwable>();


    /**
     * 完成的RPC次数
     */
    private final AtomicInteger successCount = new AtomicInteger(0);

    /**
     * 合并结果使用
     */
    private final ResultTransformer<T> resultTransformer;


    /**
     * Log
     */
    private static final Logger log = LoggerFactory.getLogger(ClientResultReceiver.class);



    /**
     * Construct a non-closed ClientResult, which waits for addResults() or
     * addError() calls until close() is called. After that point, addResults()
     * and addError() calls are ignored, and this object becomes immutable.
     *
     * @param closedListener If not null, it's clientResultClosed() method is called when our
     *                       close() method is.
     * @param allShards      The set of all shards to expect results from.
     */
    public ClientResultReceiver(IClosedListener closedListener, int nodeCount,
                                String method,
                                SolrParams params,
                                String... allShards) {
        this(closedListener, nodeCount, method, params, Arrays.asList(allShards));
    }


    /**
     * Construct a non-closed ClientResult, which waits for addResults() or
     * addError() calls until close() is called. After that point, addResults()
     * and addError() calls are ignored, and this object becomes immutable.
     *
     * @param closedListener If not null, it's clientResultClosed() method is called when our
     *                       close() method is.
     * @param allShards      The set of all shards to expect results from.
     */
    public ClientResultReceiver(IClosedListener closedListener,
                                int nodeCount,
                                String method,
                                SolrParams params,
                                Collection<String> allShards) {
        if (allShards == null || allShards.isEmpty()) {
            throw new IllegalArgumentException("No shards specified");
        }
        this.nodeCount = nodeCount;
        this.allShards = Collections.unmodifiableSet(new HashSet<String>(allShards));
        this.closedListener = closedListener;

        resultTransformer = ResultTransformerFactory.getResultTransformer(method, params);

        if (log.isTraceEnabled()) {
            log.trace(String.format("Created ClientResult(%s, %s)", closedListener != null ? closedListener : "null",
                    allShards));
        }
    }


    /**
     * Add a result. Will be ignored if closed.
     *
     * @param result The result to add.
     * @param shards The shards used to compute the result.
     */
    @Override
    public synchronized void addResult(T result, Collection<String> shards) {
        if (closed) {
            if (log.isTraceEnabled()) {
                log.trace("Ignoring results given to closed ClientResult");
            }
            return;
        }
        if (shards == null) {
            log.warn("Null shards passed to AddResult()");
            return;
        }

        resultTransformer.transform(result, shards);
        seenShards.addAll(shards);

        //次数够了, 立即结束
        if(successCount.incrementAndGet() >= nodeCount) {
            close();
        }
    }

    /**
     * Add a result. Will be ignored if closed.
     *
     * @param result The result to add.
     * @param shards The shards used to compute the result.
     */
    public synchronized void addResult(T result, String... shards) {
        addResult(result, Arrays.asList(shards));
    }

    /**
     * Add an error. Will be ignored if closed.
     *
     * @param error  The error to add.
     * @param shards The shards used when the error happened.
     */
    @Override
    public synchronized void addError(Throwable error, Collection<String> shards) {
        if (closed) {
            if (log.isTraceEnabled()) {
                log.trace("Ignoring exception given to closed ClientResult");
            }
            return;
        }
        if (shards == null) {
            log.warn("Null shards passed to addError()");
            return;
        }
        Entry entry = new Entry(error, shards, true);
        if (entry.shards.isEmpty()) {
            log.warn("Empty shards passed to addError()");
            return;
        }
        synchronized (this) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Adding error %s", entry));
            }
            Collection<String> shardListT = entry.shards;
            if (log.isWarnEnabled()) {
                for (String shard : shardListT) {
                    if (seenShards.contains(shard)) {
                        log.warn("Duplicate occurances of shard " + shard);
                    } else if (!allShards.contains(shard)) {
                        log.warn("Unknown shard " + shard + " returned results");
                    }
                }
            }
            entries.add(entry);
            seenShards.addAll(shardListT);
            if (error != null) {
                errors.add(error);
            }
        }
    }

    /**
     * Add an error. Will be ignored if closed.
     *
     * @param error  The error to add.
     * @param shards The shards used when the error happened.
     */
    public synchronized void addError(Throwable error, String... shards) {
        addError(error, Arrays.asList(shards));
    }

    /**
     * Stop accepting additional results or errors. Become an immutable object.
     * Also report the closure to the IClosedListener passed to our constructor,
     * if any. Normally this will tell the WorkQueue to shut down immediately,
     * killing any still running threads.
     */
    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            if (closedListener != null) {
                closedListener.clientResultClosed();
            }
        }
    }

    /**
     * Is this result set closed, and therefore not accepting any additional
     * results or errors. Once closed, this becomes an immutable object.
     */
    @Override
    public boolean isClosed() {
        return closed;
    }

    /**
     * @return the set of all shards we are expecting results from.
     */
    public Set<String> getAllShards() {
        return allShards;
    }

    /**
     * @return the set of shards from whom we have seen either results or errors.
     */
    public Set<String> getSeenShards() {
        return Collections.unmodifiableSet(closed ? seenShards : new HashSet<String>(seenShards));
    }

    /**
     * @return the subset of all shards from whom we have not seen either results
     *         or errors.
     */
    @Override
    public synchronized Set<String> getMissingShards() {
        Set<String> missing = new HashSet<String>(allShards);
        missing.removeAll(seenShards);
        return missing;
    }


    /**
     * @return all of the results seen so far. Does not include errors.
     */
    @Override
    public synchronized T getResult() {
        if(resultTransformer == null) {
            log.warn("null result.... ");
            return null;
        }
        return resultTransformer.getResult();
    }

    @Override
    public Collection<T> getResults() {
        T r = getResult();
        if(r == null) {
            Collections.emptyList();
        }
        return Arrays.asList(getResult());
    }

    /**
     * Either return results or throw an exception. Allows simple one line use of
     * a ClientResult. If no errors occurred, returns same results as
     * getResults(). If any errors occurred, one is chosen via getError() and
     * thrown.
     *
     * @return if no errors occurred, results via getResults().
     * @throws Throwable if any errors occurred, via getError().
     */
    public synchronized T getResultsOrThrowException() throws Throwable {
        if (isError()) {
            throw getError();
        } else {
            return getResult();
        }
    }

    /**
     * Either return results or throw a KattaException. Allows simple one line use
     * of a ClientResult. If no errors occurred, returns same results as
     * getResults(). If any errors occurred, one is chosen via getKattaException()
     * and thrown.
     *
     * @return if no errors occurred, results via getResults().
     * @throws KattaException if any errors occurred, via getError().
     */
    public synchronized T getResultsOrThrowKattaException() throws KattaException {
        if (isError()) {
            throw getKattaException();
        } else {
            return getResult();
        }
    }

    /**
     * @return all of the errors seen so far.
     */
    public Collection<Throwable> getErrors() {
        return Collections.unmodifiableCollection(closed ? errors : new ArrayList<Throwable>(errors));
    }

    /**
     * @return a randomly chosen error, or null if none exist.
     */
    public Throwable getError() {
        for (Entry<T> e : entries) {
            if (e.error != null) {
                return e.error;
            }
        }
        return null;
    }

    /**
     * @return a randomly chosen KattaException if one exists, else a
     *         KattaException wrapped around a randomly chosen error if one
     *         exists, else null.
     */
    @Override
    public KattaException getKattaException() {
        Throwable error = null;
        for (Entry e : entrySet()) {
            if (e.error != null) {
                if (e.error instanceof KattaException) {
                    return (KattaException) e.error;
                } else {
                    error = e.error;
                }
            }
        }
        if (error != null) {
            return new KattaException("Error", error);
        } else {
            return null;
        }
    }


    /**
     * @return true if we have seen either a result or an error for all shards.
     */
    @Override
    public boolean isComplete() {
        return seenShards.containsAll(allShards);
    }

    /**
     * @return true if any errors were reported.
     */
    @Override
    public boolean isError() {
        return !errors.isEmpty();
    }

    /**
     * @return true if result is complete (all shards reporting in) and no errors
     *         occurred.
     */
    public boolean isOK() {
        return isComplete() && !isError();
    }

    /**
     * @return the ratio (0.0 .. 1.0) of shards we have seen. 0.0 when no shards,
     *         1.0 when complete.
     */
    @Override
    public double getShardCoverage() {
        int seen = seenShards.size();
        int all = allShards.size();
        return all > 0 ? (double) seen / (double) all : 0.0;
    }

    /**
     * @return the time when this ClientResult was created.
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * @return a snapshot of all the data about the results so far.
     */
    public Set<Entry<T>> entrySet() {
        if (closed) {
            return Collections.unmodifiableSet(entries);
        } else {
            synchronized (this) {
                // Set will keep changing, make a snapshot.
                return Collections.unmodifiableSet(new HashSet<Entry<T>>(entries));
            }
        }
    }

    /**
     * @return a list of our results or errors, in the order they arrived.
     */
    public List<Entry> getArrivalTimes() {
        List<Entry> arrivals;
        synchronized (this) {
            arrivals = new ArrayList<Entry>(entries);
        }
        Collections.sort(arrivals, new Comparator<Entry>() {
            public int compare(Entry o1, Entry o2) {
                if (o1.time != o2.time) {
                    return o1.time < o2.time ? -1 : 1;
                } else {
                    // Break ties in favor of results.
                    if (o1.result != null && o2.result == null) {
                        return -1;
                    } else if (o2.result != null && o1.result == null) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            }
        });
        return arrivals;
    }

    @Override
    public synchronized String toString() {
        int numErrors = 0;
        for (Entry e : entrySet()) {
            if (e.error != null) {
                numErrors++;
            }
        }
        return String.format("ClientResult: %d results, %d errors, %d/%d shards%s%s",
                successCount.get(),
                numErrors,
                seenShards.size(), allShards.size(),
                closed ? " (closed)" : "", isComplete() ? " (complete)" : "");
    }

}
