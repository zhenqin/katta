package com.ivyft.katta.lib.writer;

import com.ivyft.katta.util.NodeConfiguration;
import org.apache.lucene.document.Document;

import java.util.Collection;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/28
 * Time: 12:09
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public interface DocumentFactory<T> {


    /**
     * 初始化一些信息.
     * @param conf Node Conf
     */
    public void init(NodeConfiguration conf);


    /**
     * 从 obj 对象中返回 Document
     * @param obj 对象
     * @return 返回 Lucene Document
     */
    public Collection<Document> get(T obj);
}
