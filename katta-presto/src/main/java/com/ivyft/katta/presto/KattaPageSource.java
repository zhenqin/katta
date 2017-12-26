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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.ivyft.katta.hadoop.KattaInputSplit;
import com.ivyft.katta.hadoop.KattaSocketReader;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.joda.time.chrono.ISOChronology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Preconditions.checkState;
import static com.ivyft.katta.presto.TypeUtils.*;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.stream.Collectors.toList;

public class KattaPageSource implements ConnectorPageSource {
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();

    private static final Logger log = Logger.get(KattaPageSource.class);


    /**
     * 字段名称
     */
    private final List<String> columnNames;


    /**
     * 每个字段的类型
     */
    private final List<Type> columnTypes;


    /**
     * 计数器
     */
    private final AtomicLong count = new AtomicLong(0);


    /**
     * 当前 Document
     */
    private SolrDocument currentDoc;



    private long start = System.currentTimeMillis();

    /**
     * 每次取的数据量
     */
    private int limit;

    /**
     * 是否已完成
     */
    private boolean finished;


    /**
     * Katta Reader
     */
    private KattaSocketReader reader;

    public KattaPageSource(
            KattaSession kattaSession,
            KattaSplit split,
            List<KattaColumnHandle> columns) {
        this.columnNames = columns.stream().map(KattaColumnHandle::getName).collect(toList());
        this.columnTypes = columns.stream().map(KattaColumnHandle::getType).collect(toList());
        KattaInputSplit inputSplit = split.getInputSplit();
        this.limit = inputSplit.getLimit();

        TupleDomain<ColumnHandle> tupleDomain = split.getTupleDomain();
        SolrQuery query = documentOf("*", "*");
        if (tupleDomain.getDomains().isPresent()) {
            for (Map.Entry<ColumnHandle, Domain> entry : tupleDomain.getDomains().get().entrySet()) {
                KattaColumnHandle column = (KattaColumnHandle) entry.getKey();
                String fq = buildPredicate(column, entry.getValue());
                if(StringUtils.isBlank(fq)) {
                    continue;
                }
                query.addFilterQuery(fq);
            }
        }
        inputSplit.setQuery(query);
        this.reader = new KattaSocketReader(inputSplit);
        try {
            this.reader.initialize(inputSplit);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }


    private static String buildPredicate(KattaColumnHandle column, Domain domain) {
        String name = column.getName();
        Type type = column.getType();
        if (domain.getValues().isNone() && domain.isNullAllowed()) {
            // 返回某个字段为 null, Solr 不支持
            return null;
        }
        if (domain.getValues().isAll() && !domain.isNullAllowed()) {
            // 返回某个字段不为 null
            return name + ":*";
        }

        StringBuilder fq = new StringBuilder();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            if (range.isSingleValue()) {
                return name + ":" + translateValue(range.getSingleValue(), type);
            } else {

            }
        }
        return fq.toString();
    }

    private static String translateValue(Object source, Type type) {
        if(source instanceof Slice) {
            return ((Slice) source).toStringUtf8();
        }
        return String.valueOf(source);
    }


    private static SolrQuery documentOf(String key, Object value) {
        return new SolrQuery(key + ":" + value);
    }


    @Override
    public long getCompletedBytes() {
        return count.get();
    }

    @Override
    public long getReadTimeNanos() {
        return System.currentTimeMillis() - start;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public long getSystemMemoryUsage() {
        return 0L;
    }

    @Override
    public Page getNextPage() {
        start = System.currentTimeMillis();
        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        count.set(0);

        log.info(columnNames.toString());
        log.info(columnTypes.toString());
        try {
            while (reader.nextKeyValue()) {
                currentDoc = reader.getCurrentValue();
                count.addAndGet(109);

                pageBuilder.declarePosition();
                for (int column = 0; column < columnTypes.size(); column++) {
                    BlockBuilder output = pageBuilder.getBlockBuilder(column);
                    appendTo(columnTypes.get(column), currentDoc.get(columnNames.get(column)), output);
                }

            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        finished = true;
        return pageBuilder.build();
    }



    private void appendTo(Type type, Object value, BlockBuilder output) {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        try {
            if (javaType == boolean.class) {
                type.writeBoolean(output, (Boolean) value);
            } else if (javaType == long.class) {
                if (type.equals(BIGINT)) {
                    long val = value instanceof Long ? (Long)value : Long.parseLong(String.valueOf(value));
                    type.writeLong(output, val);
                } else if (type.equals(INTEGER)) {
                    long val = value instanceof Long ? (Long)value : Long.parseLong(String.valueOf(value));
                    type.writeLong(output, val);
                } else if (type.equals(DATE)) {
                    long utcMillis = ((Date) value).getTime();
                    type.writeLong(output, TimeUnit.MILLISECONDS.toDays(utcMillis));
                } else if (type.equals(TIME)) {
                    type.writeLong(output, UTC_CHRONOLOGY.millisOfDay().get(((Date) value).getTime()));
                } else if (type.equals(TIMESTAMP)) {
                    type.writeLong(output, ((Date) value).getTime());
                } else {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for " + javaType.getSimpleName() + ":" + type.getTypeSignature());
                }
            } else if (javaType == double.class) {
                double val = value instanceof Double ? (Double)value : Double.parseDouble(String.valueOf(value));
                type.writeDouble(output, val);
            } else if (javaType == Slice.class) {
                writeSlice(output, type, value);
            } else if (javaType == Block.class) {
                writeBlock(output, type, value);
            } else {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for " + javaType.getSimpleName() + ":" + type.getTypeSignature());
            }
        } catch (ClassCastException ignore) {
            log.warn("convert value error!", ignore);
            // returns null instead of raising exception
            output.appendNull();
        }
    }

    private void writeSlice(BlockBuilder output, Type type, Object value) {
        String base = type.getTypeSignature().getBase();
        if (base.equals(StandardTypes.VARCHAR)) {
            type.writeSlice(output, utf8Slice(value instanceof String ? (String)value : String.valueOf(value)));
        } else if (type.equals(VARBINARY)) {
            output.appendNull();
        } else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private void writeBlock(BlockBuilder output, Type type, Object value) {
        if (isArrayType(type)) {
            if (value instanceof List<?>) {
                BlockBuilder builder = output.beginBlockEntry();

                ((List<?>) value).forEach(element ->
                        appendTo(type.getTypeParameters().get(0), element, builder));

                output.closeEntry();
                return;
            }
        } else if (isMapType(type)) {
            if (value instanceof List<?>) {
                BlockBuilder builder = output.beginBlockEntry();
                for (Object element : (List<?>) value) {
                    if (!(element instanceof Map<?, ?>)) {
                        continue;
                    }

                    Map<?, ?> document = (Map<?, ?>) element;
                    if (document.containsKey("key") && document.containsKey("value")) {
                        appendTo(type.getTypeParameters().get(0), document.get("key"), builder);
                        appendTo(type.getTypeParameters().get(1), document.get("value"), builder);
                    }
                }

                output.closeEntry();
                return;
            }
        } else if (isRowType(type)) {
            if (value instanceof Map) {
                Map<?, ?> mapValue = (Map<?, ?>) value;
                BlockBuilder builder = output.beginBlockEntry();
                List<String> fieldNames = type.getTypeSignature().getParameters().stream()
                        .map(TypeSignatureParameter::getNamedTypeSignature)
                        .map(NamedTypeSignature::getName)
                        .collect(Collectors.toList());
                checkState(fieldNames.size() == type.getTypeParameters().size(), "fieldName doesn't match with type size : %s", type);
                for (int index = 0; index < type.getTypeParameters().size(); index++) {
                    appendTo(type.getTypeParameters().get(index), mapValue.get(fieldNames.get(index).toString()), builder);
                }
                output.closeEntry();
                return;
            } else if (value instanceof List<?>) {
                List<?> listValue = (List<?>) value;
                BlockBuilder builder = output.beginBlockEntry();
                for (int index = 0; index < type.getTypeParameters().size(); index++) {
                    if (index < listValue.size()) {
                        appendTo(type.getTypeParameters().get(index), listValue.get(index), builder);
                    } else {
                        builder.appendNull();
                    }
                }
                output.closeEntry();
                return;
            }
        } else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Block: " + type.getTypeSignature());
        }

        // not a convertible value
        output.appendNull();
    }

    @Override
    public void close()
            throws IOException {
        if(reader != null) {
            reader.close();
        }
        reader = null;
    }
}
