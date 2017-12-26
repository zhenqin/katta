/*
 * Copyright 2010-2013 10gen Inc.
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

package com.ivyft.katta.hive;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import static java.lang.String.format;

/**
 * Used to sync documents in some Katta Index with
 * rows in a Hive table
 */
public class KattaStorageHandler extends DefaultStorageHandler implements HiveStoragePredicateHandler {
    // stores the location of the collection
    public static final String ZOOKEEPER_SERVERS = "zookeeper.servers";


    // get location of where meta-data is stored about the katta index
    public static final String TABLE_LOCATION = "location";

    private Properties properties = null;

    private static final Logger LOG = LoggerFactory.getLogger(KattaStorageHandler.class);

    @Override
    public Class<? extends InputFormat<?, ?>> getInputFormatClass() {
        return KattaMr1InputFormat.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return new KattaHiveMetaHook();
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return HiveIgnoreKeyTextOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return SolrDocumentSerde.class;
    }


    @Override
    public DecomposedPredicate decomposePredicate(
      final JobConf jobConf,
      final Deserializer deserializer,
      final ExprNodeDesc predicate) {
        SolrDocumentSerde serde = (SolrDocumentSerde) deserializer;

        // Create a new analyzer capable of handling equality and general
        // binary comparisons (false = "more than just equality").
        // expressions, but we could push down more than that in the future by
        // writing our own analyzer.
        IndexPredicateAnalyzer analyzer =
          IndexPredicateAnalyzer.createAnalyzer(false);
        // Predicate may contain any column.
        for (String colName : serde.getColumnNames()) {
            analyzer.allowColumnName(colName);
        }
        List<IndexSearchCondition> searchConditions =
          new LinkedList<IndexSearchCondition>();
        ExprNodeDesc residual = analyzer.analyzePredicate(
          predicate, searchConditions);

        DecomposedPredicate decomposed = new DecomposedPredicate();
        decomposed.pushedPredicate =
          analyzer.translateSearchConditions(searchConditions);
        decomposed.residualPredicate = (ExprNodeGenericFuncDesc) residual;
        return decomposed;
    }

    /**
     * HiveMetaHook used to define events triggered when a hive table is
     * created and when a hive table is dropped.
     */
    private class KattaHiveMetaHook implements HiveMetaHook {
        @Override
        public void preCreateTable(final Table tbl) throws MetaException {
            Map<String, String> tblParams = tbl.getParameters();
            if (!(tblParams.containsKey(ZOOKEEPER_SERVERS))) {
                throw new MetaException(
                        format("You must specify '%s' in TBLPROPERTIES", ZOOKEEPER_SERVERS));
            }
        }

        @Override
        public void commitCreateTable(final Table tbl) throws MetaException {
        }

        @Override
        public void rollbackCreateTable(final Table tbl) throws MetaException {
        }

        @Override
        public void preDropTable(final Table tbl) throws MetaException {
        }

        @Override
        public void commitDropTable(final Table tbl, final boolean deleteData) throws MetaException {
            boolean isExternal = MetaStoreUtils.isExternalTable(tbl);

            //删除表，如何操作？
            /*
            if (deleteData && !isExternal) {
                Map<String, String> tblParams = tbl.getParameters();

            }
            */
        }

        @Override
        public void rollbackDropTable(final Table tbl) throws MetaException {
        }
    }

    @Override
    public void configureInputJobProperties(final TableDesc tableDesc, final Map<String, String> jobProperties) {
        Properties properties = tableDesc.getProperties();
        copyJobProperties(properties, jobProperties);
    }

    @Override
    public void configureOutputJobProperties(final TableDesc tableDesc, final Map<String, String> jobProperties) {
        Properties properties = tableDesc.getProperties();
        copyJobProperties(properties, jobProperties);
    }

    /**
     * Helper function to copy properties
     */
    private void copyJobProperties(final Properties from, final Map<String, String> to) {
        // Copy Hive-specific properties used directly by
        if (from.containsKey(serdeConstants.LIST_COLUMNS)) {
            to.put(serdeConstants.LIST_COLUMNS,
                    (String) from.get(serdeConstants.LIST_COLUMNS));
        }
        if (from.containsKey(serdeConstants.LIST_COLUMN_TYPES)) {
            to.put(serdeConstants.LIST_COLUMN_TYPES,
                    (String) from.get(serdeConstants.LIST_COLUMN_TYPES));
        }
        if (from.containsKey(TABLE_LOCATION)) {
            to.put(TABLE_LOCATION, (String) from.get(TABLE_LOCATION));
        }

        // Copy general connector properties, such as ones defined in
        for (Entry<Object, Object> entry : from.entrySet()) {
            String key = (String) entry.getKey();
            if (key.startsWith("katta.")) {
                to.put(key, (String) from.get(key));
            }
        }

        // Update the keys for ZOOKEEPER_SERVERS per.
        if (from.containsKey("zookeeper.servers")) {
            String zkServer = (String) from.get(ZOOKEEPER_SERVERS);
            to.put(ZOOKEEPER_SERVERS, zkServer);
        }
    }

}
