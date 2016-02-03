package com.ivyft.katta.lib.lucene;

import com.ivyft.katta.util.NodeConfiguration;

import java.io.IOException;

/**
 *
 * <p>
 *     该类用于 Node Close IndexSearcher 服务.
 *
 *     如果某些 IndexSearcher 长期不用, 可以尝试关闭, 节省 Node 资源.
 * </p>
 *
 * <p>
 *     该类提供如何关闭 IndexSearcher 的策略
 * </p>
 *
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/2/2
 * Time: 12:53
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public interface CloseIndexSearcherPolicy {


    /**
     * 加载成功后, 初始化
     * @param conf NodeConf
     */
    public void init(NodeConfiguration conf);



    /**
     * 如果关闭了 IndexSearcher, 则返回 true, 否则返回 false
     * @param name Index Name
     * @param handle
     * @return 关闭 Index, 返回 true
     */
    public boolean close(String name, SearcherHandle handle) throws IOException;

}
