package com.ivyft.katta.hive;

import com.ivyft.katta.hadoop.KattaInputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.*;

import java.lang.reflect.Method;
import java.util.*;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 2017/12/20
 * Time: 09:00
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class SolrDocumentSerde extends AbstractSerDe {


    // params
    private List<String> columnNames = null;


    protected List<String> copyColumnNames = null;


    private List<TypeInfo> columnTypes = null;



    private ObjectInspector objectInspector = null;



    protected static Log LOG = LogFactory.getLog(SolrDocumentSerde.class);


    public SolrDocumentSerde() {
    }


    @Override
    public void initialize(Configuration configuration, Properties properties) throws SerDeException {
        LOG.info("serde initialize");

        LOG.info("start properties output: ");
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            LOG.info(entry.getKey() + "\t" + entry.getValue());
        }
        LOG.info("end properties output.");

        String zkServer = properties.getProperty(KattaInputFormat.ZOOKEEPER_SERVERS);
        if(StringUtils.isBlank(zkServer)) {
            throw new IllegalArgumentException("zookeeper.servers must not be null, please set 'zookeeper.servers' property.");
        }

        String index = properties.getProperty(KattaInputFormat.INDEX_NAMES);
        if(StringUtils.isBlank(index)) {
            throw new IllegalArgumentException("katta.hadoop.indexes must not be null, please set 'katta.hadoop.indexes' property.");
        }

        String query = properties.getProperty(KattaInputFormat.INPUT_QUERY);
        if(StringUtils.isBlank(query)) {
            throw new IllegalArgumentException("katta.input.query must not be null, please set 'katta.input.query' property.");
        }

        // Read Column Names
        String columnNameProp = properties.getProperty(serdeConstants.LIST_COLUMNS);

        if (columnNameProp != null && columnNameProp.length() > 0) {
            String[] splits = columnNameProp.split(",");
            columnNames = new ArrayList<String>(splits.length);
            copyColumnNames = new ArrayList<String>(splits.length);
            for (String split : splits) {
                split = StringUtils.trim(split);
                columnNames.add(split);
                copyColumnNames.add(StringUtils.upperCase(split));
            }
        } else {
            columnNames = new ArrayList<String>();
            copyColumnNames = new ArrayList<String>();
        }

        LOG.info("column: " + columnNames);


        // Read Column Types
        String columnTypeProp = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES);

        // default all string
        if (columnTypeProp == null) {
            String[] types = new String[columnNames.size()];
            Arrays.fill(types, 0, types.length, serdeConstants.STRING_TYPE_NAME);

            columnTypeProp = StringUtils.join(types, ":");
        }


        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProp);
        // Check column and types equals
        if (columnTypes.size() != columnNames.size()) {
            throw new SerDeException("len(columnNames) != len(columntTypes)");
        }

        LOG.info("column type: " + columnTypes);

        // Create ObjectInspectors from the type information for each column
        List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());

        ObjectInspector oi;
        for (int c = 0; c < columnNames.size(); c++) {
            oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(c));
            columnOIs.add(oi);
        }
        objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);


    }


    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<String> getCopyColumnNames() {
        return copyColumnNames;
    }

    public List<TypeInfo> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return MapWritable.class;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public Object deserialize(Writable wr) throws SerDeException {
        if(wr == null) {
            return null;
        }

        if (wr instanceof MapWritable) {
            MapWritable value = (MapWritable) wr;

            ArrayList<Object> row = new ArrayList<Object>(columnNames.size());

            for (String columnName : copyColumnNames) {
                // copyColumnName 已经使得大写的字母了。
                Writable writable = value.get(new Text(columnName));
                if(writable == null) {
                    row.add(null);
                } else {
                    row.add(getStructVal(writable));
                }
            }
            return row;

        }
        return null;
    }


    /**
     * 获得该 Writable 的值
     * @param writable
     * @return
     */
    public Object getStructVal(Writable writable) {
        Object value = null;
        if(writable instanceof Text){
            return ((Text)writable).toString();
        } else if(writable instanceof ArrayWritable) {
            Writable[] writables = ((ArrayWritable) writable).get();
            ArrayList<Object> innerRow = new ArrayList<Object>(columnNames.size());
            for (Writable writable1 : writables) {
                // 递归获取数组里每一个值
                innerRow.add(getStructVal(writable1));
            }

            return innerRow;
        } else {
            try {
                Method get = writable.getClass().getDeclaredMethod("get");
                return get.invoke(writable);
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }
    }


    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return this.objectInspector;
    }
}
