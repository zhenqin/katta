package com.ivyft.katta.node.io;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 14-3-24
 * Time: 上午10:53
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public class DataInputInputStream extends InputStream {
    private DataInput in;

    public static InputStream constructInputStream(DataInput in) {
        if ((in instanceof InputStream)) {
            return (InputStream) in;
        }
        return new DataInputInputStream(in);
    }

    public DataInputInputStream(DataInput in) {
        this.in = in;
    }

    public int read() throws IOException {
        return this.in.readUnsignedByte();
    }
}
