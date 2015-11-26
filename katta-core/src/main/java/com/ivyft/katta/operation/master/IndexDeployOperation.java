/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ivyft.katta.operation.master;


import com.ivyft.katta.master.DefaultDistributionPolicy;
import com.ivyft.katta.master.MasterContext;
import com.ivyft.katta.operation.FilenameNDotFilter;
import com.ivyft.katta.operation.OperationId;
import com.ivyft.katta.operation.node.OperationResult;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.IndexDeployError;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.protocol.metadata.Shard;
import com.ivyft.katta.util.HadoopUtil;
import com.ivyft.katta.util.ZkConfiguration;
import org.I0Itec.zkclient.ExceptionUtil;
import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 *
 * 给Node部署一份索引。
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-19
 * Time: 上午8:59
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class IndexDeployOperation extends AbstractIndexOperation {

    /**
     * 序列化
     */
    private static final long serialVersionUID = 1L;


    /**
     * 索引信息
     */
    protected IndexMetaData indexMD;


    /**
     * shard名称
     */
    private String indexName;


    /**
     * 索引存放地址
     */
    private String indexPath;


    /**
     * Solr collectionName
     */
    private String collectionName;


    /**
     * Log
     */
    private final static Logger LOG = LoggerFactory.getLogger(IndexDeployOperation.class);


    /**
     * 构造方法
     * @param indexName 索引名称
     * @param indexPath 索引地址
     * @param replicationLevel 复制份数
     */
    public IndexDeployOperation(String indexName, String indexPath,
                                String collectionName, int replicationLevel) {
        this.indexMD = new IndexMetaData(indexName, indexPath, collectionName, replicationLevel);
        this.collectionName = collectionName;
        this.indexName = indexName;
        this.indexPath = indexPath;
    }

    public String getIndexName() {
        return this.indexName;
    }

    public String getIndexPath() {
        return this.indexPath;
    }


    public String getCollectionName() {
        return collectionName;
    }

    public int getReplicationLevel() {
        return this.indexMD.getReplicationLevel();
    }

    @Override
    public List<OperationId> execute(MasterContext context,
                                     List<MasterOperation> runningOperations) throws Exception {
        InteractionProtocol protocol = context.getProtocol();
        try {
            //从文件系统遍历所有需要的shard目录下的索引
            this.indexMD.getShards().addAll(readShardsFromFs(this.indexName, this.indexPath, this.collectionName));

            LOG.info("Found shards '" + this.indexMD.getShards() +
                    "' for index '" + this.indexName +
                    "' use solr collectionName '" + this.collectionName +
                    "'");

            //创建一个分布式的计划,给每一个node分发索引
            List<OperationId> operationIds = distributeIndexShards(context, this.indexMD, protocol.getLiveNodes(),
                    runningOperations);
            return operationIds;
        } catch (Exception e) {
            ExceptionUtil.rethrowInterruptedException(e);
            LOG.error("failed to deploy index " + this.indexName, e);
            protocol.publishIndex(this.indexMD);
            handleMasterDeployException(protocol, this.indexMD, e);
            return null;
        }
    }

    @Override
    public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
        LOG.info("deployment of index " + this.indexName + " complete");
        handleDeploymentComplete(context, results, this.indexMD, true);
    }

    @Override
    public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
        for (MasterOperation operation : runningOperations) {
            if (operation instanceof IndexDeployOperation &&
                    ((IndexDeployOperation) operation).indexName.equals(this.indexName)) {
                return ExecutionInstruction.CANCEL;
            }
        }
        return ExecutionInstruction.EXECUTE;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + Integer.toHexString(hashCode()) + ":" + this.indexName;
    }


    /**
     * 该方法从文件系统中遍历所有的一个shard中所有的目录数或者zip文件, 每个目录/zip文件都将是一个shard
     *
     * @param indexName shard名称
     * @param indexPathString shard存放的Path
     * @return 返回shardPath下所有的目录.
     * @throws IndexDeployException
     */
    protected static List<Shard> readShardsFromFs(String indexName,
                                                  String indexPathString,
                                                  String collectionName)
            throws IndexDeployException {
        // get shard folders from source
        URI uri;
        try {
            uri = new URI(indexPathString);
        } catch (URISyntaxException e) {
            throw new IndexDeployException(IndexDeployError.ErrorType.INDEX_NOT_ACCESSIBLE, "unable to parse index path uri '"
                    + indexPathString + "', make sure it starts with file:// or hdfs:// ", e);
        }
        FileSystem fileSystem;
        try {
            fileSystem = HadoopUtil.getFileSystem(new Path(uri.toString()));
        } catch (IOException e) {
            throw new IndexDeployException(IndexDeployError.ErrorType.INDEX_NOT_ACCESSIBLE,
                    "unable to retrive file system for index path '"
                    + indexPathString + "', make sure your path starts with hadoop support prefix like file:// or hdfs://", e);
        }

        List<Shard> shards = new ArrayList<Shard>();
        try {
            Path indexPath = new Path(indexPathString);
            if (!fileSystem.exists(indexPath)) {
                throw new IndexDeployException(IndexDeployError.ErrorType.INDEX_NOT_ACCESSIBLE, "index path '" + uri + "' does not exists");
            }

            //取得前缀不是.的文件夹或者Zip文件
            FileStatus[] listStatus = fileSystem.listStatus(indexPath, new FilenameNDotFilter(fileSystem, true));
            for (FileStatus fileStatus : listStatus) {
                String shardPath = fileStatus.getPath().toString();
                if (fileStatus.isDir() || shardPath.endsWith(".zip")) {

                    //遍历所有的zip或者目录加入shard
                    shards.add(new Shard(createShardName(indexName, shardPath), shardPath));
                }
            }
        } catch (IOException e) {
            throw new IndexDeployException(IndexDeployError.ErrorType.INDEX_NOT_ACCESSIBLE,
                    "could not access index path: " + indexPathString,
                    e);
        }

        if (shards.size() == 0) {
            throw new IndexDeployException(IndexDeployError.ErrorType.INDEX_NOT_ACCESSIBLE,
                    "index does not contain any shard");
        }
        return shards;
    }



    public static void main(String[] args) throws Exception {
        ZkClient zkClient = new ZkClient("zhenqin-pro102:2181", 6000, 6000);
        InteractionProtocol protocol = new InteractionProtocol(zkClient, new ZkConfiguration());
        IndexDeployOperation operation = new IndexDeployOperation("test", "path", "core", 2);

        IndexMetaData data = new IndexMetaData("test", "path", "core", 4);
        data.addShard(new Shard("index1", "path1"));
        data.addShard(new Shard("index2", "path2"));

        List<String> nodes = new ArrayList<String>();
        for (int i = 0; i < 20; i++) {
            nodes.add("node-" + i);
        }
        MasterContext context = new MasterContext(protocol, null, new DefaultDistributionPolicy(), null);

        operation.distributeIndexShards(context, data, nodes, Arrays.<MasterOperation>asList(operation));
    }
}
