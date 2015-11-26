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
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import org.I0Itec.zkclient.ExceptionUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 *
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
public class BalanceIndexOperation extends AbstractIndexOperation {


    /**
     * 序列化表格
     */
    private static final long serialVersionUID = 1L;


    /**
     * shardName。索引地址？
     */
    private String indexName;


    /**
     * Log
     */
    private final static Logger LOG = LoggerFactory.getLogger(BalanceIndexOperation.class);


    /**
     *
     * @param indexName 索引名称
     */
    public BalanceIndexOperation(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
        //TODO 平衡索引，这里也很重要
        InteractionProtocol protocol = context.getProtocol();
        IndexMetaData indexMD = protocol.getIndexMD(indexName);
        if (indexMD == null) {
        // could be undeployed in meantime
            LOG.info("skip balancing for index '" + indexName + "' cause it is already undeployed");
            return null;
        }
        if (!canAndShouldRegulateReplication(protocol, indexMD)) {
            LOG.info("skip balancing for index '" + indexName + "' cause there is no possible optimization");
            return null;
        }
        try {
            FileSystem fileSystem = context.getFileSystem(indexMD);
            Path path = new Path(indexMD.getPath());
            if (!fileSystem.exists(path)) {
                LOG.warn("skip balancing for index '" + indexName + "' cause source '" + path + "' does not exists anymore");
                return null;
            }
        } catch (Exception e) {
            LOG.error("skip balancing of index '" + indexName + "' cause failed to access source '" + indexMD.getPath()
                    + "'", e);
            return null;
        }

        LOG.info("balancing shards for index '" + indexName + "'");
        try {
            List<OperationId> operationIds = distributeIndexShards(context, indexMD, protocol.getLiveNodes(),
                    runningOperations);
            return operationIds;
        } catch (Exception e) {
            ExceptionUtil.rethrowInterruptedException(e);
            LOG.error("failed to deploy balance " + indexName, e);
            handleMasterDeployException(protocol, indexMD, e);
            return null;
        }
    }

    @Override
    public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
        LOG.info("balancing of index " + indexName + " complete");
        IndexMetaData indexMD = context.getProtocol().getIndexMD(indexName);
        if (indexMD != null) {// could be undeployed in meantime
            handleDeploymentComplete(context, results, indexMD, false);
        }
    }

    @Override
    public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
        for (MasterOperation operation : runningOperations) {
            if (operation instanceof BalanceIndexOperation
                    && ((BalanceIndexOperation) operation).indexName.equals(indexName)) {
                return ExecutionInstruction.CANCEL;
            }
        }
        return ExecutionInstruction.EXECUTE;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + Integer.toHexString(hashCode()) + ":" + indexName;
    }
}
