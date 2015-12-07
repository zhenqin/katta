package com.ivyft.katta.lib.writer;

import java.io.Serializable;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/7
 * Time: 11:56
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class ShardRange implements Serializable {


    /**
     * 序列化
     */
    private final static long serialVersionUID = 0L;


    private int start;


    private int end;


    private String shardName;


    private String shardPath;


    public ShardRange(String shardName, String shardPath) {
        this.shardName = shardName;
        this.shardPath = shardPath;
    }


    public ShardRange(int start, int end) {
        this.start = start;
        this.end = end;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public String getShardName() {
        return shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    public String getShardPath() {
        return shardPath;
    }

    public void setShardPath(String shardPath) {
        this.shardPath = shardPath;
    }


    @Override
    public String toString() {
        return "ShardRange{" +
                "start=" + start +
                ", end=" + end +
                ", shardName='" + shardName + '\'' +
                ", shardPath='" + shardPath + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShardRange range = (ShardRange) o;

        return (range.start >= start) && (range.end <= end);

    }

    @Override
    public int hashCode() {
        int result = start;
        result = 31 * result + end;
        return result;
    }
}
