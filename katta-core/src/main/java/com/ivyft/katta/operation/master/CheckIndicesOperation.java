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
import com.ivyft.katta.protocol.metadata.IndexDeployError;
import com.ivyft.katta.protocol.metadata.IndexDeployError.ErrorType;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-13
 * Time: 上午8:58
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class CheckIndicesOperation extends AbstractIndexOperation {


    /**
     * 序列化表格
     */
    private static final long serialVersionUID = 1L;



    /**
     * Log
     */
    private static final Logger LOG = LoggerFactory.getLogger(CheckIndicesOperation.class);



    /**
     * 构造方法
     */
    public CheckIndicesOperation() {

    }

    @Override
    public List<OperationId> execute(MasterContext context,
                                     List<MasterOperation> runningOperations)
            throws Exception {
        InteractionProtocol protocol = context.getProtocol();
        List<String> liveNodes = protocol.getLiveNodes();
        LOG.info("liveNodes: " + liveNodes);

        List<String> indices = protocol.getIndices();
        LOG.info("indices: " + indices);

        for (String indexName : indices) {
            IndexMetaData indexMD = protocol.getIndexMD(indexName);
            LOG.info(indexMD.toString());
            if (indexMD != null) {
                // can be removed already
                if ((indexMD.hasDeployError() && isRecoverable(indexMD.getDeployError(), liveNodes.size()))
                        || canAndShouldRegulateReplication(protocol, indexMD)) {

                    LOG.info("balance shard index, shard name: " + indexMD.getName() + " index path: " + indexMD.getPath());
                    protocol.addMasterOperation(new BalanceIndexOperation(indexName));
                }
            }
        }
        return null;
    }

    private boolean isRecoverable(IndexDeployError deployError, int nodeCount) {
        if (deployError.getErrorType() == ErrorType.NO_NODES_AVAILIBLE && nodeCount > 0) {
            return true;
        }
        return false;
    }

    @Override
    public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
        // nothing to do
    }

    @Override
    public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
        for (MasterOperation operation : runningOperations) {
            if (operation instanceof CheckIndicesOperation) {
                return ExecutionInstruction.ADD_TO_QUEUE_TAIL;
            }
        }
        return ExecutionInstruction.EXECUTE;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + Integer.toHexString(hashCode());
    }
}
