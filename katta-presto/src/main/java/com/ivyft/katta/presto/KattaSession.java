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

import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.ivyft.katta.client.INodeProxyManager;
import com.ivyft.katta.lib.lucene.FieldInfoWritable;
import com.ivyft.katta.lib.lucene.ILuceneServer;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.protocol.metadata.Shard;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;


/**
 *
 *
 * @author zhenqin
 */
public class KattaSession {
    private static final Logger log = Logger.get(KattaSession.class);


    private final TypeManager typeManager;


    private final InteractionProtocol protocol;


    protected final INodeProxyManager proxyManager;



    public KattaSession(TypeManager typeManager,
                        InteractionProtocol protocol,
                        INodeProxyManager proxyManager) {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.protocol = requireNonNull(protocol, "protocol is null");
        this.proxyManager = requireNonNull(proxyManager, "manager is null");
    }


    /**
     * Presto Shutdown
     */
    public void shutdown() {
        protocol.disconnect();

        proxyManager.shutdown();
    }


    /**
     * 获取所有的数据库，Katta  没有数据库的概念，因此列出几个默认的数据库。
     * @return
     */
    public List<String> getAllSchemas() {
        return ImmutableList.of("default", "katta", "");
    }


    /**
     * 列出该数据库下的所有的表，Katta 意味着索引
     * @param schema
     * @return
     * @throws SchemaNotFoundException
     */
    public Set<String> getAllTables(String schema) throws SchemaNotFoundException {
        List<String> indices = protocol.getIndices();
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();

        builder.addAll(indices);

        return builder.build();
    }


    /**
     * 获取 Katta Index 详细信息，字段 and 字段类型等
     * @param tableName
     * @return
     * @throws TableNotFoundException
     */
    public KattaTable getTable(SchemaTableName tableName) throws TableNotFoundException {
        return loadTableSchema(tableName);
    }


    private KattaTable loadTableSchema(SchemaTableName tableName)
            throws TableNotFoundException {
        String db = tableName.getSchemaName();
        String tb = tableName.getTableName();

        IndexMetaData indexMD = protocol.getIndexMD(tb);
        Set<Shard> shards = indexMD.getShards();
        if(shards.isEmpty()) {
            throw new IllegalArgumentException();
        }
        Shard shard = Lists.newArrayList(shards).get(0);

        List<String> shardNodes = protocol.getShardNodes(shard.getName());

        // 随机的选择一个该 shard 的负载节点 Node
        int r = new Random().nextInt(shardNodes.size());
        String node = shardNodes.get(r);

        // 字段和索引
        ImmutableList.Builder<KattaColumnHandle> columnHandles = ImmutableList.builder();
        ImmutableList.Builder<KattaIndex> indexes = ImmutableList.builder();
        try {
            //String jar = ClassInJarUtils.findContainingJar(Writable.class);

            ILuceneServer server = (ILuceneServer)proxyManager.getProxy(node, true);
            requireNonNull(server, "rpc server is null");

            log.info("got shard name: " + shard.getName() + " on node " + node);
            FieldInfoWritable fieldsInfo = server.getFieldsInfo(shard.getName());
            List<List<Object>> result = fieldsInfo.getResult();
            for (List<Object> objects : result) {
                String fieldName = (String)objects.get(0);
                if("_version_".equals(fieldName)) {
                    continue;
                }
                columnHandles.add(new KattaColumnHandle(
                        fieldName,
                        translateValue((String)objects.get(1)),
                        false));

                indexes.add(new KattaIndex(fieldName,
                        ImmutableList.of(new KattaIndex.KattaIndexKey(fieldName, SortOrder.ASC_NULLS_LAST)),
                        false));
            }

        } catch (IOException e) {
            log.error(e);
        }

        KattaTableHandle tableHandle = new KattaTableHandle(tableName);
        return new KattaTable(tableHandle, columnHandles.build(), indexes.build());
    }


    /**
     * 根据 Solr 的类型 str 翻译为 presto 的数据类型
     * @param source
     * @return
     */
    private static Type translateValue(String source) {
        switch (source) {
            case "string":
                return VarcharType.VARCHAR;
            case "int":
                return IntegerType.INTEGER;
            case "long":
                return DecimalType.createDecimalType(String.valueOf(Long.MAX_VALUE).length(), 0);
            case "short":
                return DecimalType.createDecimalType(String.valueOf(Short.MAX_VALUE).length(), 0);
            case "float":
                return DecimalType.createDecimalType(5, 5);
            case "double":
                return DecimalType.createDecimalType(12, 5);
            case "date":
                return DateType.DATE;
            case "tdate":
                return TimestampType.TIMESTAMP;
            case "boolean":
                return BOOLEAN;
            default:
                return VarcharType.VARCHAR;
        }
    }
}
