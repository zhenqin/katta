package com.ivyft.katta.lib.writer;

import com.ivyft.katta.protocol.IntLengthHeaderFile;
import com.ivyft.katta.util.HadoopUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/6
 * Time: 14:18
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class SerializationReader {


    private IntLengthHeaderFile.Reader reader;


    private SerdeContext serdeContext;


    protected final static Logger LOG = LoggerFactory.getLogger(SerializationReader.class);


    public SerializationReader(IntLengthHeaderFile.Reader reader) {
        this.reader = reader;
        try {
            readSerdeContext();
        } catch (IOException e) {
            throw new IllegalStateException("读取序列化上下文出错, 或者该文件不是 Katta 生成的序列化文件.", e);
        }
    }


    protected void readSerdeContext() throws IOException {
        SerdeContext context = new SerdeContext();
        if(serdeContext == null) {
            int available = reader.available();

            reader.hasNext();
            byte[] next = reader.next();

            DataInputStream in = new DataInputStream(new ByteArrayInputStream(next));
            context.readFields(in);
            LOG.info("Serialization class: " + context.getSerClass());
            context.setSize(available);

        }
        this.serdeContext = context;
    }


    public ByteBuffer nextByteBuffer() throws IOException {
        if(reader.hasNext()) {
            return reader.nextByteBuffer();
        }

        throw new EOFException("end file.");
    }

    public SerdeContext getSerdeContext() {
        return serdeContext;
    }


    public void close() throws IOException {
        reader.close();
    }


    public static void main(String[] args) throws IOException {
        Path p = new Path("/user/katta/data/test/2/bggtf09-ojih65f-151207004525-data.dat");
        IntLengthHeaderFile.Reader reader = new IntLengthHeaderFile.Reader(HadoopUtil.getFileSystem(p), p);
        SerializationReader r = new SerializationReader(reader);
        System.out.println(r.getSerdeContext());
    }
}
