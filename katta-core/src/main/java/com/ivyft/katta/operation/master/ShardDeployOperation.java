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

import com.ivyft.katta.master.MasterContext;
import com.ivyft.katta.operation.OperationId;
import com.ivyft.katta.operation.node.OperationResult;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.IndexDeployError.ErrorType;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.protocol.metadata.Shard;
import com.ivyft.katta.util.HadoopUtil;
import org.I0Itec.zkclient.ExceptionUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
public class ShardDeployOperation extends AbstractIndexOperation {

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
    private final String indexName;


    /**
     * 索引存放地址
     */
    private final String shardPath;



    /**
     * Solr collectionName
     */
    private final String collectionName;


    /**
     * Log
     */
    private final static Logger LOG = LoggerFactory.getLogger(ShardDeployOperation.class);


    /**
     * 构造方法
     * @param indexName 索引名称
     * @param shardPath 索引地址
     */
    public ShardDeployOperation(String indexName, String shardPath,
                                String collectionName, int replicationLevel) {
        this.indexMD = new IndexMetaData(indexName, shardPath, collectionName, replicationLevel);
        this.indexName = indexName;
        this.collectionName = collectionName;
        this.shardPath = shardPath;
    }





    @Override
    public List<OperationId> execute(MasterContext context,
                                     List<MasterOperation> runningOperations) throws Exception {
        InteractionProtocol protocol = context.getProtocol();
        try {
            //从文件系统遍历所有需要的shard目录下的索引
            this.indexMD.getShards().add(readShardFromFs(this.indexName, this.shardPath, this.collectionName));

            LOG.info("Found shards '" + this.indexMD.getShards() +
                    "' for index '" + this.indexName +
                    "'");

            //创建一个分布式的计划,给每一个node分发索引
            //添加shard到Index
            List<OperationId> operationIds = distributeIndexShards(context,
                    this.indexMD,
                    protocol.getLiveNodes(),
                    runningOperations);
            return operationIds;
        } catch (Exception e) {
            ExceptionUtil.rethrowInterruptedException(e);
            LOG.error("failed to deploy index " + this.indexName + " shards: " + indexMD.getShards(), e);
            IndexMetaData data = protocol.getIndexMD(indexName);
            if(data != null) {
                data.addShards(indexMD.getShards());
            }
            handleMasterDeployException(protocol, data, e);
            return null;
        }
    }


    /**
     * 部署成功了，调用该方法
     * @param context MasterContext
     * @param results 执行结果
     * @throws Exception 可能抛出的异常
     */
    @Override
    public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
        LOG.info("deployment of shard " + shardPath + " to " + this.indexName + " complete");
        IndexMetaData data = context.getProtocol().getIndexMD(indexName);
        if(data != null) {
            data.addShards(indexMD.getShards());
        }
        handleDeploymentComplete(context, results, data, false);
    }

    @Override
    public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
        for (MasterOperation operation : runningOperations) {
            if (operation instanceof ShardDeployOperation &&
                    ((ShardDeployOperation) operation).indexName.equals(this.indexName)) {
                return ExecutionInstruction.CANCEL;
            }
        }
        return ExecutionInstruction.EXECUTE;
    }



    public String getIndexName() {
        return this.indexName;
    }

    public String getShardPath() {
        return shardPath;
    }

    public int getReplicationLevel() {
        return this.indexMD.getReplicationLevel();
    }


    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + Integer.toHexString(hashCode()) +
                ": deploy " + shardPath + " to index: " + this.indexName;
    }


    /**
     * 该方法从文件系统中遍历所有的一个shard中所有的目录数或者zip文件, 每个目录/zip文件都将是一个shard
     *
     * @param indexName shard名称
     * @param shardPathString shard存放的Path
     * @return 返回shardPath下所有的目录.
     * @throws com.ivyft.katta.operation.master.IndexDeployException
     */
    protected Shard readShardFromFs(final String indexName,
                                    final String shardPathString,
                                    final String collectionName)
            throws IndexDeployException {
        // get shard folders from source
        URI uri;
        try {
            uri = new URI(shardPathString);
        } catch (final URISyntaxException e) {
            throw new IndexDeployException(ErrorType.INDEX_NOT_ACCESSIBLE, "unable to parse index path uri '"
                    + shardPathString + "', make sure it starts with file:// or hdfs:// ", e);
        }
        FileSystem fileSystem;
        try {
            fileSystem = HadoopUtil.getFileSystem(new Path(uri.toString()));
        } catch (final IOException e) {
            throw new IndexDeployException(ErrorType.INDEX_NOT_ACCESSIBLE,
                    "unable to retrive file system for index path '"
                    + shardPathString +
                            "', make sure your path starts with hadoop support prefix like file:// or hdfs://", e);
        }
        try {
            final Path shardPath = new Path(shardPathString);
            if (!fileSystem.exists(shardPath)) {
                throw new IndexDeployException(ErrorType.INDEX_NOT_ACCESSIBLE,
                        "shard path '" + uri + "' does not exists");
            }
            if (fileSystem.isFile(shardPath)){
                throw new IndexDeployException(ErrorType.INDEX_NOT_ACCESSIBLE,
                        "shard path '" + uri + "' must Directory");
            }

            return new Shard(createShardName(indexName, shardPathString), shardPathString);
        } catch (final IOException e) {
            throw new IndexDeployException(ErrorType.INDEX_NOT_ACCESSIBLE,
                    "could not access index path: " + shardPathString,
                    e);
        }
    }

}
