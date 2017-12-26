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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;


/**
 * 该表才是很关键的
 */
public class KattaMetadata implements ConnectorMetadata {
    private static final Logger log = Logger.get(KattaMetadata.class);

    private final KattaSession kattaSession;

    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    public KattaMetadata(KattaSession kattaSession) {
        this.kattaSession = requireNonNull(kattaSession, "kattaSession is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return kattaSession.getAllSchemas();
    }


    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
        log.info("find " + schemaNameOrNull + " 's tables.");
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();

        for (String tableName : kattaSession.getAllTables(schemaNameOrNull)) {
            tableNames.add(new SchemaTableName(schemaNameOrNull, tableName.toLowerCase(ENGLISH)));
        }
        return tableNames.build();
    }



    @Override
    public KattaTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        requireNonNull(tableName, "tableName is null");
        log.info("get table " + tableName.toString() + " 's handle.");
        try {
            return kattaSession.getTable(tableName).getTableHandle();
        } catch (TableNotFoundException e) {
            log.debug(e, "Table(%s) not found", tableName);
            return null;
        }
    }



    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
        requireNonNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = ((KattaTableHandle) tableHandle).getSchemaTableName();
        return getTableMetadata(session, tableName);
    }


    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName) {
        KattaTableHandle tableHandle = kattaSession.getTable(tableName).getTableHandle();

        List<ColumnMetadata> columns = ImmutableList.copyOf(
                getColumnHandles(session, tableHandle).values().stream()
                        .map(KattaColumnHandle.class::cast)
                        .map(KattaColumnHandle::toColumnMetadata)
                        .collect(toList()));

        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        KattaTableHandle table = (KattaTableHandle) tableHandle;
        List<KattaColumnHandle> columns = kattaSession.getTable(table.getSchemaTableName()).getColumns();

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (KattaColumnHandle columnHandle : columns) {
            columnHandles.put(columnHandle.getName(), columnHandle);
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(session, tableName).getColumns());
            } catch (NotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return ((KattaColumnHandle) columnHandle).toColumnMetadata();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
        KattaTableHandle tableHandle = (KattaTableHandle) table;

        Optional<Set<ColumnHandle>> partitioningColumns = Optional.empty();
        ImmutableList.Builder<LocalProperty<ColumnHandle>> localProperties = ImmutableList.builder();

        KattaTable tableInfo = kattaSession.getTable(tableHandle.getSchemaTableName());
        Map<String, ColumnHandle> columns = getColumnHandles(session, tableHandle);

        for (KattaIndex index : tableInfo.getIndexes()) {
            for (KattaIndex.KattaIndexKey key : index.getKeys()) {
                if (!key.getSortOrder().isPresent()) {
                    continue;
                }
                if (columns.get(key.getName()) != null) {
                    localProperties.add(new SortingProperty<>(columns.get(key.getName()), key.getSortOrder().get()));
                }
            }
        }

        ConnectorTableLayout layout = new ConnectorTableLayout(
                new KattaTableLayoutHandle(tableHandle, constraint.getSummary()),
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty(),
                partitioningColumns,
                Optional.empty(),
                localProperties.build());

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        KattaTableLayoutHandle layout = (KattaTableLayoutHandle) handle;

        // tables in this connector have a single layout
        return getTableLayouts(session, layout.getTable(), Constraint.alwaysTrue(), Optional.empty())
                .get(0)
                .getTableLayout();
    }



    public void rollback() {
        Optional.ofNullable(rollbackAction.getAndSet(null)).ifPresent(Runnable::run);
    }



    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix) {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    private static List<KattaColumnHandle> buildColumnHandles(ConnectorTableMetadata tableMetadata) {
        return tableMetadata.getColumns().stream()
                .map(m -> new KattaColumnHandle(m.getName(), m.getType(), m.isHidden()))
                .collect(toList());
    }
}
