package com.ivyft.katta.client;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-12-18
 * Time: 下午1:47
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public interface INodeInteractionFactory<T> {

    public Runnable createInteraction(Method method,
                                      Object[] args,
                                      int shardArrayParamIndex,
                                      String node,
                                      Map<String, List<String>> nodeShardMap,
                                      int tryCount,
                                      int maxTryCount,
                                      INodeProxyManager shardManager,
                                      INodeExecutor nodeExecutor,
                                      IResultReceiver<T> results);
}
