package com.ivyft.katta.lib.writer;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/29
 * Time: 10:31
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class SerdeContext {

    protected final String serdeName;


    protected final String serClass;


    protected final long size;


    public SerdeContext(String serdeName, String serClass, long size) {
        this.serdeName = serdeName;
        this.serClass = serClass;
        this.size = size;
    }


    public String getSerdeName() {
        return serdeName;
    }

    public String getSerClass() {
        return serClass;
    }

    public long getSize() {
        return size;
    }


    @Override
    public String toString() {
        return "SerdeContext{" +
                "serdeName='" + serdeName + '\'' +
                ", serClass='" + serClass + '\'' +
                ", size=" + size +
                '}';
    }
}
