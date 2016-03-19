package com.ivyft.katta.lib.lucene;

import com.ivyft.katta.lib.lucene.group.GroupCollectorFactory;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.queries.function.valuesource.FieldCacheSource;
import org.apache.lucene.search.SortField;
import org.apache.solr.schema.SchemaField;

import java.io.Serializable;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 14-1-9
 * Time: 下午7:12
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class TypeSource implements Serializable {


    /**
     * 序列化表格
     */
    private final static long serialVersionUID = 1L;


    /**
     * Lucene内部使用解析成Java的类型解析器
     */
    protected FieldCacheSource fieldCacheSource;


    /**
     * 字段数值类型对应的Hadoop Writable类型
     */
    protected Writable simpleValue;


    /**
     * 用于解析该字段的值成为Writable类型的一个转换器工厂类
     */
    protected GroupCollectorFactory collectorFactory;


    /**
     * 字段类型
     */
    protected SchemaField schemaField;


    /**
     * 字段排序类型
     */
    protected SortField.Type fieldType;


    /**
     *
     * 构造方法
     *
     * @param fieldCacheSource 字段对应的Lucene内部解析的CacheSource
     * @param schemaField 字段类型
     * @param simpleValue 字段所属类型的Writable样例类型
     */
    public TypeSource(FieldCacheSource fieldCacheSource,
                      SchemaField schemaField,
                      SortField.Type fieldType,
                      GroupCollectorFactory collectorFactory,
                      Writable simpleValue) {
        this.fieldCacheSource = fieldCacheSource;
        this.simpleValue = simpleValue;
        this.fieldType = fieldType;
        this.schemaField = schemaField;
        this.collectorFactory = collectorFactory;
    }


    public FieldCacheSource getFieldCacheSource() {
        return fieldCacheSource;
    }


    public Writable getSimpleValue() {
        return simpleValue;
    }

    public void setSimpleValue(Writable simpleValue) {
        this.simpleValue = simpleValue;
    }


    public GroupCollectorFactory getCollectorFactory() {
        return collectorFactory;
    }

    public SchemaField getSchemaField() {
        return schemaField;
    }


    public SortField.Type getFieldType() {
        return fieldType;
    }
}
