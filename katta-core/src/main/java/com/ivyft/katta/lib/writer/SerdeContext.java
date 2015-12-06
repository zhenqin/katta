package com.ivyft.katta.lib.writer;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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

    protected String serdeName;


    protected String serClass;


    protected long size;


    public SerdeContext() {
    }

    public SerdeContext(String serdeName, String serClass) {
        this.serdeName = serdeName;
        this.serClass = serClass;
        this.size = -1;
    }

    public SerdeContext(String serdeName, String serClass, long size) {
        this.serdeName = serdeName;
        this.serClass = serClass;
        this.size = size;
    }


    public void setSerdeName(String serdeName) {
        this.serdeName = serdeName;
    }

    public void setSerClass(String serClass) {
        this.serClass = serClass;
    }

    public void setSize(long size) {
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



    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.getSerdeName());
        out.writeUTF(this.getSerClass());
        out.writeLong(this.size);
    }

    public void readFields(DataInput in) throws IOException {
        this.serdeName = in.readUTF();
        this.serClass = in.readUTF();
        this.size = in.readInt();
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
