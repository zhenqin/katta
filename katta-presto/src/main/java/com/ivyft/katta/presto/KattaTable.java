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

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class KattaTable {
    private final KattaTableHandle tableHandle;
    private final List<KattaColumnHandle> columns;
    private final List<KattaIndex> indexes;

    public KattaTable(KattaTableHandle tableHandle,
                      List<KattaColumnHandle> columns,
                      List<KattaIndex> indexes) {
        this.tableHandle = tableHandle;
        this.columns = ImmutableList.copyOf(columns);
        this.indexes = ImmutableList.copyOf(indexes);
    }

    public KattaTableHandle getTableHandle() {
        return tableHandle;
    }

    public List<KattaColumnHandle> getColumns() {
        return columns;
    }

    public List<KattaIndex> getIndexes() {
        return indexes;
    }

    @Override
    public int hashCode() {
        return tableHandle.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof KattaTable)) {
            return false;
        }
        KattaTable that = (KattaTable) obj;
        return this.tableHandle.equals(that.tableHandle);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("tableHandle", tableHandle)
                .toString();
    }
}
