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
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.ivyft.katta.hadoop.KattaInputFormat;
import com.ivyft.katta.hadoop.KattaInputSplit;
import com.ivyft.katta.hadoop.KattaSpliter;
import com.ivyft.katta.protocol.InteractionProtocol;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class KattaSplitManager implements ConnectorSplitManager {
    private static final Logger log = Logger.get(KattaSplitManager.class);



    private final InteractionProtocol protocol;



    private final Configuration conf;

    @Inject
    public KattaSplitManager(Configuration conf, InteractionProtocol protocol) {
        this.protocol = protocol;
        this.conf = conf;
        requireNonNull(protocol, "protocol is null");
        requireNonNull(conf,     "conf is null");
    }


    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout) {
        return getSplits(transactionHandle, session, layout, SplitSchedulingStrategy.GROUPED_SCHEDULING);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
                                          ConnectorSession session,
                                          ConnectorTableLayoutHandle layout,
                                          SplitSchedulingStrategy splitSchedulingStrategy) {
        KattaTableLayoutHandle tableLayout = (KattaTableLayoutHandle) layout;
        KattaTableHandle tableHandle = tableLayout.getTable();
        SchemaTableName tableName = tableHandle.getSchemaTableName();

        KattaInputFormat.setIndexNames(conf, new String[]{tableName.getTableName()});
        List<KattaInputSplit> inputSplits = KattaSpliter.calculateSplits(conf);
        ImmutableList.Builder<KattaSplit> builder = ImmutableList.builder();
        for (KattaInputSplit split : inputSplits) {
            KattaSplit kattaSplit = new KattaSplit(
                    tableHandle.getSchemaTableName(),
                    tableLayout.getTupleDomain(),
                    Arrays.asList(HostAddress.fromParts(split.getHost(), split.getPort())),
                    split.getKeyField(),
                    split.getShardName(),
                    split.getStart(),
                    split.getMaxDocs(),
                    split.getLimit());

            builder.add(kattaSplit);
        }

        return new FixedSplitSource(builder.build());
    }
}
