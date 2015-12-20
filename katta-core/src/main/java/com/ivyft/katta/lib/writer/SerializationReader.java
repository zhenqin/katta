package com.ivyft.katta.lib.writer;

import com.ivyft.katta.codec.Serializer;
import com.ivyft.katta.protocol.IntLengthHeaderFile;
import com.ivyft.katta.util.HadoopUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
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


    private final SerdeContext serdeContext;


    protected final static Logger LOG = LoggerFactory.getLogger(SerializationReader.class);


    public SerializationReader(IntLengthHeaderFile.Reader reader) {
        this.reader = reader;
        try {
            SerdeContext context = new SerdeContext();
            int available = reader.available();

            reader.hasNext();
            byte[] next = reader.next();

            DataInputStream in = new DataInputStream(new ByteArrayInputStream(next));
            context.readFields(in);
            LOG.info("Serialization class: " + context.getSerClass());
            context.setSize(available);

            this.serdeContext = context;
        } catch (IOException e) {
            throw new IllegalStateException("读取序列化上下文出错, 或者该文件不是 Katta 生成的序列化文件.", e);
        }
    }


    public ByteBuffer nextByteBuffer() throws IOException {
        if(reader.hasNext()) {
            return reader.nextByteBuffer();
        }
        return null;
    }

    public SerdeContext getSerdeContext() {
        return serdeContext;
    }


    public void close() throws IOException {
        reader.close();
    }


    public static void main(String[] args) throws Exception {
        Path p = new Path("/user/katta/data/hello/4Nx6WBy6nO6QsMr1Hok/commit-20151220205414/dfbc784bfd7e-20151220205410-data.dat");
        IntLengthHeaderFile.Reader reader = new IntLengthHeaderFile.Reader(HadoopUtil.getFileSystem(p), p);
        SerializationReader r = new SerializationReader(reader);
        SerdeContext context = r.getSerdeContext();
        System.out.println(context);

        Class<Serialization> aClass = (Class<Serialization>) Class.forName(context.getSerClass());
        Serializer serializer = aClass.newInstance().serialize();

        int count = 1;
        ByteBuffer byteBuffer = r.nextByteBuffer();
        while (byteBuffer != null) {
            System.out.println(count + "   " + serializer.deserialize(byteBuffer.array()));
            count++;

            byteBuffer = r.nextByteBuffer();
        }
        r.close();
    }
}
