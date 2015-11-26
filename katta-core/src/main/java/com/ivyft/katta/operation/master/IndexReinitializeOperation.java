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
public class IndexReinitializeOperation extends IndexDeployOperation {

    public IndexReinitializeOperation(IndexMetaData indexMD) {
        super(indexMD.getName(), indexMD.getPath(), indexMD.getCollectionName(), indexMD.getReplicationLevel());
    }

    @Override
    public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
        InteractionProtocol protocol = context.getProtocol();
        try {
            this.indexMD.getShards().addAll(readShardsFromFs(this.indexMD.getName(),
                    this.indexMD.getPath(),
                    this.indexMD.getCollectionName()));

            protocol.updateIndexMD(this.indexMD);
        } catch (Exception e) {
            ExceptionUtil.rethrowInterruptedException(e);
            handleMasterDeployException(protocol, this.indexMD, e);
        }
        return null;
    }

    @Override
    public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
        return ExecutionInstruction.EXECUTE;
    }

    @Override
    public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
        // nothing to do
    }

}
