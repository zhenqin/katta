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
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.ivyft.katta.hadoop.KattaInputSplit;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KattaSplit implements ConnectorSplit {
    private final SchemaTableName schemaTableName;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final List<HostAddress> addresses;


    private KattaInputSplit inputSplit;

    @JsonCreator
    public KattaSplit(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("keyField") String keyField,
            @JsonProperty("shardName") String shardName,
            @JsonProperty("start") int start,
            @JsonProperty("maxDocs") int maxDocs,
            @JsonProperty("limit") int limit
            ) {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));

        this.inputSplit = new KattaInputSplit();
        this.inputSplit.setKeyField(keyField);
        this.inputSplit.setShardName(shardName);
        this.inputSplit.setStart(start);
        this.inputSplit.setMaxDocs(maxDocs);
        this.inputSplit.setLimit(limit);
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName() {
        return schemaTableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain() {
        return tupleDomain;
    }

    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    @Override
    @JsonProperty
    public List<HostAddress> getAddresses() {
        return addresses;
    }

    @JsonProperty
    public String getShardName() {
        return inputSplit.getShardName();
    }

    @JsonProperty
    public String getKeyField() {
        return inputSplit.getKeyField();
    }

    @JsonProperty
    public int getLimit() {
        return inputSplit.getLimit();
    }

    @JsonProperty
    public int getStart() {
        return inputSplit.getStart();
    }

    @JsonProperty
    public int getMaxDocs() {
        return inputSplit.getMaxDocs();
    }


    public KattaInputSplit getInputSplit() {
        HostAddress address = addresses.get(0);
        inputSplit.setHost(address.getHostText());
        inputSplit.setPort(address.getPort());
        return inputSplit;
    }

    @Override
    public Object getInfo() {
        return this;
    }
}
