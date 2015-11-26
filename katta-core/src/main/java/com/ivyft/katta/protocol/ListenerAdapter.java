package com.ivyft.katta.protocol;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-19
 * Time: 上午10:06
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class ListenerAdapter {


    /**
     * ZooKeeper上改变的节点
     */
    private final String path;


    /**
     * 构造方法传入改变的节点
     *
     * @param path 节点
     */
    public ListenerAdapter(String path) {
        this.path = path;
    }


    public final String getPath() {
        return this.path;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + getPath();
    }
}
