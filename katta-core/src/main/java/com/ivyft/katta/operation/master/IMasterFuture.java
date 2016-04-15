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

/**
 * Future for an Master Operation.
 *
 * @see java.util.concurrent.Future for concept of a future
 */
public interface IMasterFuture {
    public static enum State {
        STOPPED, ERROR, RUNNINGING;

    }


    /**
     * 等到操作，直到完成
     *
     * @return
     */
    State joinDeployment() throws InterruptedException;

    /**
     * 在指定的时间内等待
     * @return
     */
    State joinDeployment(long maxWaitMillis) throws InterruptedException;


    /**
     * 获得当前的状态
     * @return 返回当前状态
     */
    State getState();



    void disposable();
}
