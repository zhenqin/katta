package com.ivyft.katta.client;

import com.ivyft.katta.client.transformer.*;

import java.util.HashMap;
import java.util.Map;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-12-20
 * Time: 下午1:16
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public class ResultTransformerFactory {


    /**
     * 每个方法对应一个集合器，用来收集结果
     */
    private final Map<String, Class<? extends ResultTransformer>>
            resultTransformerMapper = new HashMap<String, Class<? extends ResultTransformer>>(10);


    /**
     * 合并结果
     */
    private static ResultTransformerFactory factory;


    /**
     * 私有构造方法
     */
    private ResultTransformerFactory() {
        resultTransformerMapper.put("query", QueryResponseTransformer.class);
        resultTransformerMapper.put("search", HitsTransformer.class);
        resultTransformerMapper.put("getDetail", MapWritableTransformer.class);
        resultTransformerMapper.put("getDetails", ArrayMapWritableTransformer.class);
        resultTransformerMapper.put("count", TotalTransformer.class);
        resultTransformerMapper.put("get", TextArrayTransformer.class);
        resultTransformerMapper.put("group", GroupResultWritableTransformer.class);
        resultTransformerMapper.put("facet", FacetResultWritableTransformer.class);
        resultTransformerMapper.put("facetByRange", FacetResultWritableTransformer.class);
    }


    /**
     * 静态工厂方法，获得给类的实例。该类为单例模式
     * @return 返回唯一实例
     */
    public static ResultTransformerFactory getInstance() {
        if(factory == null) {
            factory = new ResultTransformerFactory();
        }
        return factory;
    }


    /**
     * 返回一个方法对应的集合器
     * @param key 方法名
     * @return 通过默认构造实例化类的对象
     */
    public <T> ResultTransformer<T> get(Object key) {
        Class<? extends ResultTransformer> transformerClass = resultTransformerMapper.get(key);
        try {
            return transformerClass.newInstance();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }


    /**
     * 获得一个集合器的实例
     * @param name 集合器名字
     * @return 返回集合器
     */
    public static ResultTransformer getResultTransformer(String name) {
        return getInstance().get(name);
    }
}
