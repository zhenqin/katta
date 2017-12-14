package com.ivyft.katta.client;

import com.ivyft.katta.Katta;
import com.ivyft.katta.lib.lucene.ILuceneServer;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.ClientConfiguration;
import com.ivyft.katta.util.ZkConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 17/12/14
 * Time: 15:54
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaAdmin extends Client {


    public KattaAdmin() {
        super(ILuceneServer.class);
    }

    public KattaAdmin(ZkConfiguration config) {
        super(ILuceneServer.class, config);
    }

    public KattaAdmin(InteractionProtocol protocol) {
        super(ILuceneServer.class, protocol);
    }

    public KattaAdmin(INodeSelectionPolicy nodeSelectionPolicy) {
        super(ILuceneServer.class, nodeSelectionPolicy);
    }

    public KattaAdmin(INodeSelectionPolicy policy, ZkConfiguration zkConfig) {
        super(ILuceneServer.class, policy, zkConfig);
    }

    public KattaAdmin(INodeSelectionPolicy policy, ZkConfiguration zkConfig, ClientConfiguration clientConfiguration) {
        super(ILuceneServer.class, policy, zkConfig, clientConfiguration);
    }

    public KattaAdmin(INodeSelectionPolicy policy, InteractionProtocol protocol, ClientConfiguration clientConfiguration) {
        super(ILuceneServer.class, policy, protocol, clientConfiguration);
    }


    public void addShard(String indexName, String path) {
        if(StringUtils.isBlank(indexName) || StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("(-i or --index) and (-p or --path) must not be null.");
        }

        Katta.addShard(this.protocol, indexName, path);
    }


    public void removeShard(String indexName, String shardName) {
        if(StringUtils.isBlank(indexName)) {
            throw new IllegalArgumentException("-i or --index must not be null.");
        }

        if(StringUtils.isBlank(shardName)) {
            throw new IllegalArgumentException("-S or --shard must not be null.");
        }

        Katta.removeShard(this.protocol, indexName, shardName);
    }


    public void addIndex(String indexName, String collectionName, String path, int replicationLevel) {
        if(StringUtils.isBlank(indexName) || StringUtils.isBlank(path) || StringUtils.isBlank(collectionName)) {
            throw new IllegalArgumentException("(-i or --index) and (-p or --path) and (-c or --core) must not be null.");
        }

        Katta.addIndex(this.protocol, indexName, collectionName, path, replicationLevel);
    }



    public void removeIndex(String indexName) {
        if(StringUtils.isBlank(indexName)) {
            throw new IllegalArgumentException("(-i or --index) must not be null.");
        }

        Katta.removeIndex(this.protocol, indexName);
    }
}
