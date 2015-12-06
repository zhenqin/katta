package com.ivyft.katta.lib.writer;

import com.ivyft.katta.protocol.IntLengthHeaderFile;
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
    }


    protected void readSerdeContext() throws IOException {
        SerdeContext context = new SerdeContext();
        if(serdeContext == null) {

            byte[] next = reader.next();

            DataInputStream in = new DataInputStream(new ByteArrayInputStream(next));
            context.readFields(in);
            LOG.info("Serialization class: " + context.getSerClass());
        }
        this.serdeContext = context;
    }


    public boolean hasNext() {
        return reader.hasNext();
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
}
