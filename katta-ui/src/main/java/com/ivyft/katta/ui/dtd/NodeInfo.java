package com.ivyft.katta.ui.dtd;

import com.ivyft.katta.protocol.metadata.NodeMetaData;

import java.util.Collection;
import java.util.Date;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 17/12/17
 * Time: 14:30
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class NodeInfo {
    private Collection<String> shards;


    protected NodeMetaData nodeMD;

    /**
     * 节点是否还在活着
     */
    protected String live = "live";

    public NodeInfo(NodeMetaData nodeMD) {
        this.nodeMD  = nodeMD;
    }


    public String getName() {
        return nodeMD.getName();
    }

    public long getStartTimeStamp() {
        return nodeMD.getStartTimeStamp();
    }

    public Date getStartTime() {
        return nodeMD.getStartTime();
    }

    public float getQueriesPerMinute() {
        return nodeMD.getQueriesPerMinute();
    }

    public NodeMetaData getNodeMD() {
        return nodeMD;
    }

    public void setNodeMD(NodeMetaData nodeMD) {
        this.nodeMD = nodeMD;
    }

    public void setShards(Collection<String> shards) {
        this.shards = shards;
    }


    public Collection<String> getShards() {
        return shards;
    }


    public int getShardSize() {
        return shards == null ? 0 : shards.size();
    }

    public String getLive() {
        return live;
    }

    public void setLive(String live) {
        this.live = live;
    }
}
