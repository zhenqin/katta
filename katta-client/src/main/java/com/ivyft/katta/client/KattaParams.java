package com.ivyft.katta.client;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 17/12/16
 * Time: 21:02
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public final class KattaParams {


    public final static String KATTA_SORT_FIELD_TYPE = "sort.fields.type";


    public static enum Type {
        STRING(String.class.getSimpleName()),
        INTEGER(Integer.class.getSimpleName()),
        LONG(Long.class.getSimpleName()),
        FLOAT(Float.class.getSimpleName()),
        SHORT(Short.class.getSimpleName()),
        DOUBLE(Double.class.getSimpleName()),
        BOOLEAN(Boolean.class.getSimpleName()),
        BYTE(Byte.class.getSimpleName());

        private String type;

        Type(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }
}
