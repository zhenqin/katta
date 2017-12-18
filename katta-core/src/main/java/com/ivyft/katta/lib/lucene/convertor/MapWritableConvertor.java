package com.ivyft.katta.lib.lucene.convertor;

import org.apache.hadoop.io.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;

import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-12-19
 * Time: 下午12:38
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class MapWritableConvertor implements DocumentConvertor<MapWritable> {


    public MapWritableConvertor() {

    }

    @Override
    public MapWritable convert(Document document, float score) {
        //所有的结果都要存放在result变量中
        final MapWritable result = new MapWritable();
        result.put(new Text("score"), new FloatWritable(score));

        //i遍历所有的field
        final List<IndexableField> fields = document.getFields();
        for (final IndexableField field : fields) {
            final String name = field.name();

            if(field.stringValue() == null) {
                continue;
            }

            //判断各种数据类型
            if (field.fieldType().docValueType() == FieldInfo.DocValuesType.BINARY) {
                final byte[] binaryValue = field.binaryValue().clone().bytes;
                result.put(new Text(name), new BytesWritable(binaryValue));
            } if (field.fieldType().docValueType() == FieldInfo.DocValuesType.NUMERIC) {
                final Number value = field.numericValue();
                if(value instanceof Integer) {
                    result.put(new Text(name), new IntWritable(value.intValue()));
                } else if(value instanceof Long) {
                    result.put(new Text(name), new LongWritable(value.longValue()));
                } else if(value instanceof Double) {
                    result.put(new Text(name), new DoubleWritable(value.doubleValue()));
                } else if(value instanceof Float) {
                    result.put(new Text(name), new FloatWritable(value.floatValue()));
                } else  if(value instanceof Short) {
                    result.put(new Text(name), new IntWritable(value.intValue()));
                } else {
                    result.put(new Text(name), new IntWritable(value.intValue()));
                }
            } else {
                final String stringValue = field.stringValue();
                result.put(new Text(name), new Text(stringValue));
            }
        }
        return result;
    }
}
