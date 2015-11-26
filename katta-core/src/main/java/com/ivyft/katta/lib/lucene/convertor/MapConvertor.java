package com.ivyft.katta.lib.lucene.convertor;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class MapConvertor implements DocumentConvertor<Map> {


    public MapConvertor() {

    }

    @Override
    public Map convert(Document document) {
        //所有的结果都要存放在result变量中
        final Map<String, Object> result = new HashMap<String, Object>();

        //i遍历所有的field
        final List<IndexableField> fields = document.getFields();
        for (final IndexableField field : fields) {
            final String name = field.name();

            if(field.stringValue() == null) {
                continue;
            }

            //判断各种数据类型
            if (field.fieldType().docValueType() == FieldInfo.DocValuesType.BINARY) {
                result.put(name, field.stringValue());
            } if (field.fieldType().docValueType() == FieldInfo.DocValuesType.NUMERIC) {
                result.put(name, field.numericValue());
            } else {
                result.put(name, field.stringValue());
            }
        }
        return result;
    }
}
