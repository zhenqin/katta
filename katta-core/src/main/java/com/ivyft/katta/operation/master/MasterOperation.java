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

import java.io.Serializable;
import java.util.List;

/**
 * An operation carried out by the master node.
 * <p/>
 * If an {@link InterruptedException} is thrown during the operations
 * {@link #execute(com.ivyft.katta.master.MasterContext, List)}
 * method (which can happen during master change
 * or zookeeper reconnect) the operation can either catch and handle or rethrow
 * it. Rethrowing it will lead to complete reexecution of the operation.
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
public interface MasterOperation extends Serializable {

    static enum ExecutionInstruction {
        EXECUTE, CANCEL, ADD_TO_QUEUE_TAIL;
    }

    /**
     * Called before {@link #execute(MasterContext, List)} to evaluate if this
     * operation is blocked, delayed, etc by another running
     * {@link MasterOperation}.
     *
     * @param runningOperations
     * @return instruction
     * @throws Exception
     */
    ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception;

    /**
     *
     * @param context
     * @param runningOperations currently running {@link MasterOperation}s
     * @return null or a list of operationId which have to be completed before
     *         {@link #nodeOperationsComplete(com.ivyft.katta.master.MasterContext, List)} method is called.
     *
     *
     * @throws Exception
     */
    List<OperationId> execute(MasterContext context,
                              List<MasterOperation> runningOperations) throws Exception;



    /**
     *
     * Called when all operations are complete or the nodes of the incomplete
     * operations went down. This method is NOT called if
     * {@link #execute(com.ivyft.katta.master.MasterContext, List)} returns null or an emptu list of
     * {@link OperationId}s.
     *
     *
     * @param context
     * @param results
     */
    void nodeOperationsComplete(MasterContext context,
                                List<OperationResult> results) throws Exception;

}
