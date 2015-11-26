package com.ivyft.katta.protocol.metadata;

import java.io.Serializable;
import java.util.*;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-19
 * Time: 下午12:28
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class Shard  implements Serializable {


    /**
     * 序列化
     */
    private static final long serialVersionUID = 1L;


    /**
     * name
     */
    private final String name;


    /**
     * path
     */
    private final String path;


    /**
     * 其他信息
     */
    private final Map<String, String> metaDataMap = new HashMap<String, String>();


    /**
     * 构造方法
     * @param name name
     * @param path path
     */
    public Shard(String name, String path) {
        this.name = name;
        this.path = path;
    }

    public String getName() {
        return this.name;
    }

    public String getPath() {
        return this.path;
    }

    public Map<String, String> getMetaDataMap() {
        return this.metaDataMap;
    }

    @Override
    public String toString() {
        return "name: " + getName() + ", path: " + getPath();
    }

    public static List<String> getShardNames(Collection<Shard> shards) {
        List<String> shardNames = new ArrayList<String>(shards.size());
        for (Shard shard : shards) {
            shardNames.add(shard.getName());
        }
        return shardNames;
    }
}
