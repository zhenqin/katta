package com.ivyft.katta.node.io;

import java.io.*;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 14-3-24
 * Time: 上午10:52
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public class UnbufferedDataInputInputStream extends org.apache.solr.common.util.DataInputInputStream {
    private final DataInputStream in;

    public UnbufferedDataInputInputStream(DataInput in) {
        this.in = new DataInputStream(DataInputInputStream.constructInputStream(in));
    }

    public void readFully(byte[] b) throws IOException {
        this.in.readFully(b);
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        this.in.readFully(b, off, len);
    }

    public int skipBytes(int n) throws IOException {
        return this.in.skipBytes(n);
    }

    public boolean readBoolean() throws IOException {
        return this.in.readBoolean();
    }

    public byte readByte() throws IOException {
        return this.in.readByte();
    }

    public int readUnsignedByte() throws IOException {
        return this.in.readUnsignedByte();
    }

    public short readShort() throws IOException {
        return this.in.readShort();
    }

    public int readUnsignedShort() throws IOException {
        return this.in.readUnsignedShort();
    }

    public char readChar() throws IOException {
        return this.in.readChar();
    }

    public int readInt() throws IOException {
        return this.in.readInt();
    }

    public long readLong() throws IOException {
        return this.in.readLong();
    }

    public float readFloat() throws IOException {
        return this.in.readFloat();
    }

    public double readDouble() throws IOException {
        return this.in.readDouble();
    }

    public String readLine() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(this.in, "UTF-8"));
        return reader.readLine();
    }

    public String readUTF() throws IOException {
        return this.in.readUTF();
    }

    public int read() throws IOException {
        return this.in.read();
    }
}
