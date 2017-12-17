package com.ivyft.katta.ui.dtd;

import com.ivyft.katta.protocol.metadata.IndexDeployError;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.protocol.metadata.Shard;

import java.util.Set;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 17/12/17
 * Time: 14:53
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class IndexInfo {

    protected IndexMetaData indexMD;



    public IndexInfo(IndexMetaData indexMD) {
        this.indexMD = indexMD;
    }


    public String getPath() {
        return indexMD.getPath();
    }

    public String getCollectionName() {
        return indexMD.getCollectionName();
    }

    public int getReplicationLevel() {
        return indexMD.getReplicationLevel();
    }

    public String getName() {
        return indexMD.getName();
    }

    public Set<Shard> getShards() {
        return indexMD.getShards();
    }


    public int getShardSize() {
        return indexMD.getShards() == null ?
                0 : indexMD.getShards().size();
    }

    public IndexDeployError getDeployError() {
        return indexMD.getDeployError();
    }

    public boolean isDeployErrorInfo() {
        return indexMD.hasDeployError();
    }
}
