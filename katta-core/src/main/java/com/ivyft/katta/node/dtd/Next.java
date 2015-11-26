package com.ivyft.katta.node.dtd;

import java.io.Serializable;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-10-8
 * Time: 上午9:49
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public class Next implements Serializable {


    /**
     * 序列化表格
     */
    private final static long serialVersionUID = 1L;


    /**
     * 当前遍历到文档的位移
     */
    private int start = 0;

    /**
     * 一次性提取的文档数量
     */
    private short limit = 1000;


    /**
     * 默认的start. 0
     * @param start
     */
    public Next(int start) {
        this.start = start;
    }


    /**
     * 默认的limit=1000
     * @param start
     * @param limit
     */
    public Next(int start, short limit) {
        this.start = start;
        this.limit = limit;
    }


    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public short getLimit() {
        return limit;
    }

    public void setLimit(short limit) {
        this.limit = limit;
    }


    @Override
    public String toString() {
        return "start: [" + start + "], limit:["+limit+"]";
    }
}
