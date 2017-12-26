/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ivyft.katta.presto;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public class KattaHandleResolver implements ConnectorHandleResolver {
    public KattaHandleResolver() {
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass() {
        return KattaTransactionHandle.class;
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return KattaTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        return KattaColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
        return KattaSplit.class;
    }


    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
        return KattaTableLayoutHandle.class;
    }
}
