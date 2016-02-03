package com.ivyft.katta.lib.lucene;

import com.ivyft.katta.util.NodeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 *
 * <p>
 *     永不关闭 IndexSearcher 实现
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
public class NoCloseIndexSearcherPolicy implements CloseIndexSearcherPolicy {




    @Override
    public void init(NodeConfiguration conf) {
    }




    @Override
    public boolean close(String name, SearcherHandle handle) throws IOException {
        return false;
    }
}
