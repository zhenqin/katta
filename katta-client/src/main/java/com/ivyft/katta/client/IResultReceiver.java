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
package com.ivyft.katta.client;

import com.ivyft.katta.util.KattaException;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * These are the only ClientResult methods NodeInteraction is allowed to call.
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
public interface IResultReceiver<T> {

    /**
     * @return true if the result is closed, and therefore not accepting any new
     *         results.
     */
    public boolean isClosed();



    public boolean isComplete();


    public boolean isError();


    public KattaException getKattaException();



    public void close();


    public double getShardCoverage();

    /**
     * Add the shard's results. Silently fails if result is closed.
     * @param methodName method
     * @param result The result to add.
     * @param shards The shards that were called to produce the result.
     */
    public void addResult(String methodName, T result, Collection<String> shards);

    /**
     * Report an error thrown by the node when we tried to access the specified
     * shards. Silently fails if result is closed.
     *
     * @param error The result to add.
     * @param shards The shards that were called to produce the result.
     */
    public void addError(Throwable error, Collection<String> shards);



    public T getResult();



    public Collection<T> getResults();



    public Set<String> getMissingShards();
}


/**
 * Immutable storage of either a result or an error, which shards produced it,
 * and it's arrival time.
 */
class Entry<T> {

    public final T result;
    public final Throwable error;
    public final Set<String> shards;
    public final long time;

    protected Entry(Object o, Collection<String> shards, boolean isError) {
        this.result = !isError ? (T) o : null;
        this.error = isError ? (Throwable) o : null;
        this.shards = Collections.unmodifiableSet(new HashSet<String>(shards));
        this.time = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        String resultStr;
        if (result != null) {
            resultStr = "null";
            if (result != null) {
                try {
                    resultStr = result.toString();
                } catch (Throwable t) {
                    resultStr = "(toString() err)";
                }
            }
            if (resultStr == null) {
                resultStr = "(null toString())";
            }
        } else {
            resultStr = error != null ? error.getClass().getSimpleName() : "null";
        }
        return String.format("%s from %s at %d", resultStr, shards, time);
    }
}
