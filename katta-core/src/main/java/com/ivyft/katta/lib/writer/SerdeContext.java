package com.ivyft.katta.lib.writer;


import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

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


    public final static int VERSION_1 = 1;



    public final static int VERSION_2 = 2;



    enum  Version {
        Version_U(VERSION_1, 128),


        Version_1(VERSION_1, 128),


        Version_2(VERSION_2, 1024);


        private int version;


        private int metaSize;


        Version(int version, int metaSize) {
            this.version = version;
            this.metaSize = metaSize;
        }

        Version() {

        }

        public int getVersion() {
            return version;
        }

        public int getMetaSize() {
            return metaSize;
        }



        public void write(DataOutput out) throws IOException {
            out.writeInt(this.version);
            out.writeInt(this.metaSize);
        }

        public void readFields(DataInput in) throws IOException {
            this.version = in.readInt();
            this.metaSize = in.readInt();
        }


        @Override
        public String toString() {
            return "Version{" +
                    "version=" + version +
                    ", metaSize=" + metaSize +
                    '}';
        }
    }



    public Version version = Version.Version_2;



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


    public Version getVersion() {
        return version;
    }

    public void write(DataOutput out) throws IOException {
        version.write(out);

        if(version.getVersion() == Version.Version_1.getVersion()) {
            out.writeUTF(this.getSerdeName());
            out.writeUTF(this.getSerClass());
            out.writeLong(this.size);
        } else if(version.getVersion() == Version.Version_2.getVersion()) {
            ByteBuffer buffer = ByteBuffer.allocate(version.getMetaSize());

            byte[] bytes = this.getSerdeName().getBytes("UTF-8");
            buffer.putInt(bytes.length);
            buffer.put(bytes);

            bytes = this.getSerClass().getBytes("UTF-8");
            buffer.putInt(bytes.length);
            buffer.put(bytes);

            buffer.putLong(size);

            while (buffer.position() < buffer.capacity()) {
                buffer.put((byte)0);
            }

            buffer.flip();
            out.write(buffer.array());
        }

    }

    public void readFields(DataInput in) throws IOException {
        Version version = Version.Version_U;
        version.readFields(in);
        this.version = version;

        if(version.getVersion() == Version.Version_1.getVersion()) {
            this.serdeName = in.readUTF();
            this.serClass = in.readUTF();
            this.size = in.readInt();
        } else if(version.getVersion() == Version.Version_2.getVersion()) {
            ByteBuffer buffer = ByteBuffer.allocate(version.getMetaSize());
            byte[] bytes = new byte[version.getMetaSize()];
            in.readFully(bytes);
            buffer.put(bytes);
            buffer.flip();

            int anInt = buffer.getInt();
            bytes = new byte[anInt];
            buffer.get(bytes);
            this.serdeName = new String(bytes, "UTF-8");

            anInt = buffer.getInt();
            bytes = new byte[anInt];
            buffer.get(bytes);
            this.serClass = new String(bytes, "UTF-8");

            this.size = buffer.getLong();
        }

    }


    @Override
    public String toString() {
        return "SerdeContext{" +
                "version=" + version +
                ", serdeName='" + serdeName + '\'' +
                ", serClass='" + serClass + '\'' +
                ", size=" + size +
                '}';
    }


    public static void main(String[] args) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(out);
        SerdeContext context = new SerdeContext("JDK", "Hello", 1000);

        context.write(outputStream);
        outputStream.flush();


        System.out.println(out.toByteArray().length);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        DataInputStream inputStream = new DataInputStream(in);
        SerdeContext serdeContext = new SerdeContext();
        serdeContext.readFields(inputStream);

        System.out.println(serdeContext);
    }

}
