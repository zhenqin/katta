package com.ivyft.katta.lib.lucene;

import com.ivyft.katta.lib.lucene.group.GroupCollectorFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.*;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrFieldSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-27
 * Time: 下午3:14
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class SolrHandler {


    /**
     * ShardName
     */
    protected final String shardName;


    /**
     * Solr Collection
     */
    protected final String collectionName;

    /**
     * Solr Core
     */
    protected final SolrCore solrCore;



    /**
     * Solr Core home
     */
    private static File solrHome = null;


    /**
     * Solr Container
     */
    private static CoreContainer container;


    /**
     * Log
     */
    private static Logger LOG = LoggerFactory.getLogger(SolrHandler.class);



    /**
     * Constructor
     *
     * @param shardName shardName
     * @param collectionName collectionName
     */
    public SolrHandler(String shardName,  String collectionName) {
        this.shardName = shardName;
        this.collectionName = collectionName;
        solrCore = container.getCore(collectionName);

        if(solrCore == null) {
            throw new IllegalArgumentException(collectionName + "'s solr core is null.");
        }
    }


    /**
     * @param solrHome SolrHome, a local dir
     */
    public static void init(File solrHome) throws Exception {
        String tmpHome = System.getProperty("solr.solr.home");
        if(StringUtils.isBlank(tmpHome)) {
            SolrHandler.solrHome = solrHome;

            System.setProperty("solr.solr.home",
                    SolrHandler.solrHome.getAbsolutePath());
        } else {
            SolrHandler.solrHome = new File(tmpHome);
        }

        LOG.info("solr.solr.home: " + SolrHandler.solrHome.getAbsolutePath());

        if(container == null) {
            LOG.info("start init solr core container.");
            container = new CoreContainer(SolrHandler.solrHome.getAbsolutePath());
            container.load();
            LOG.info("init success! end init solr core container.");
        }

        Collection<String> coreNames = container.getAllCoreNames();
        if(coreNames == null || coreNames.isEmpty()) {
            throw new IllegalStateException(SolrHandler.solrHome.getAbsolutePath()
                    + " not available solr core collections.");
        }
    }


    public String getShardName() {
        return shardName;
    }


    public String getCollectionName() {
        return collectionName;
    }


    public File getSolrHome() {
        return solrHome;
    }


    public String getSolrHomeAbsPath() {
        return solrHome.getAbsolutePath();
    }

    public SolrCore getSolrCore() {
        return solrCore;
    }


    /**
     * 取得字段Field能Group的FieldCacheSource
     * @param field Solr中配置字段名
     * @return 返回Field
     */
    public TypeSource getFieldGroup(String field) {
        SchemaField schemaField = solrCore.getLatestSchema().getField(field);

        FieldType.NumericType numericType = schemaField.getType().getNumericType();
        if(numericType != null) {
            if(StringUtils.equals(numericType.name(), "INT")) {
                return new TypeSource(new IntFieldSource(field),schemaField,
                        new GroupCollectorFactory.IntCollectorFactory(),
                        new IntWritable());
            } else if(StringUtils.equals(numericType.name(), "LONG")) {
                return new TypeSource(new LongFieldSource(field), schemaField,
                        new GroupCollectorFactory.LongCollectorFactory(),
                        new LongWritable());
            } else if(StringUtils.equals(numericType.name(), "FLOAT")){
                return new TypeSource(new FloatFieldSource(field), schemaField,
                        new GroupCollectorFactory.FloatCollectorFactory(),
                        new FloatWritable());
            } else if(StringUtils.equals(numericType.name(), "DOUBLE")){
                return new TypeSource(new DoubleFieldSource(field), schemaField,
                        new GroupCollectorFactory.DoubleCollectorFactory(),
                        new DoubleWritable());
            } else if(StringUtils.equals(numericType.name(), "DATE")){
                return new TypeSource(new LongFieldSource(field), schemaField,
                        new GroupCollectorFactory.LongCollectorFactory(),
                        new LongWritable());
            }
            throw new IllegalStateException(field + " is not a group(facet) type.");
        }
        String typeString = schemaField.getType().getTypeName();
        if(StringUtils.equals(typeString, "string")) {
            return new TypeSource(new StrFieldSource(field), schemaField,
                    new GroupCollectorFactory.TextCollectorFactory(),
                    new Text());
        } else if(StringUtils.equals(typeString, "int")) {
            return new TypeSource(new IntFieldSource(field), schemaField,
                    new GroupCollectorFactory.IntCollectorFactory(),
                    new IntWritable());
        } else if(StringUtils.equals(typeString, "long")) {
            return new TypeSource(new LongFieldSource(field), schemaField,
                    new GroupCollectorFactory.LongCollectorFactory(),
                    new LongWritable());
        } else if(StringUtils.equals(typeString, "tdate")) {
            return new TypeSource(new LongFieldSource(field), schemaField,
                    new GroupCollectorFactory.LongCollectorFactory(),
                    new LongWritable());
        } else if(StringUtils.equals(typeString, "date")) {
            return new TypeSource(new LongFieldSource(field), schemaField,
                    new GroupCollectorFactory.LongCollectorFactory(),
                    new LongWritable());
        } else if(StringUtils.equals(typeString, "double")) {
            return new TypeSource(new DoubleFieldSource(field), schemaField,
                    new GroupCollectorFactory.DoubleCollectorFactory(),
                    new DoubleWritable());
        } else if(StringUtils.equals(typeString, "float")) {
            return new TypeSource(new FloatFieldSource(field), schemaField,
                    new GroupCollectorFactory.FloatCollectorFactory(),
                    new FloatWritable());
        }
        throw new IllegalStateException(field + " type " +
                typeString + ", it's not a group(facet) type.");
    }



    public boolean isNumberStep(SolrQuery solrQuery, String rangeField) {
        String s = solrQuery.get(FacetParams.FACET_RANGE_START); //初始值
        String e = solrQuery.get(FacetParams.FACET_RANGE_END);   //结束值
        String g = solrQuery.get(FacetParams.FACET_RANGE_GAP);   //步增区间
        return StringUtils.isNotBlank(rangeField) && StringUtils.isNotBlank(s) &&
                StringUtils.isNotBlank(e) && StringUtils.isNotBlank(g);
    }



    public boolean isDateStep(SolrQuery solrQuery, String rangeField) {
        String s = solrQuery.get(FacetParams.FACET_DATE_START); //初始值
        String e = solrQuery.get(FacetParams.FACET_DATE_END);   //结束值
        String g = solrQuery.get(FacetParams.FACET_DATE_GAP);   //步增区间
        return StringUtils.isNotBlank(rangeField) && StringUtils.isNotBlank(s) &&
                StringUtils.isNotBlank(e) && StringUtils.isNotBlank(g);
    }



    public boolean isDate(SolrQuery solrQuery, String rangeField,
                          SchemaField schemaField) {
        String typeString = schemaField.getType().getTypeName();
        return StringUtils.equals(typeString, "tdate") ||
                StringUtils.equals(typeString, "date");
    }


    @Override
    public String toString() {
        return "SolrHandler{" +
                "solrHome=" + solrHome +
                ", shardName='" + shardName + '\'' +
                '}';
    }
}
