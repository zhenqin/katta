package com.ivyft.katta.lib.writer;

import com.ivyft.katta.protocol.IntLengthHeaderFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.ServiceLoader;

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
public class SerializationWriter {


    private final IntLengthHeaderFile.Writer writer;


    private boolean notWriteSerdeContext = true;


    protected final static Logger LOG = LoggerFactory.getLogger(SerializationWriter.class);


    public SerializationWriter(IntLengthHeaderFile.Writer writer) {
        this.writer = writer;
    }


    protected void writeSerdeContext() throws IOException {
        if(notWriteSerdeContext) {
            Iterator iterator = ServiceLoader.load(Serialization.class).iterator();
            Serialization serialization = (Serialization)iterator.next();

            LOG.info("Serialization class: " + serialization.getClass().getName());

            SerdeContext context = new SerdeContext(serialization.getContentType(),
                    serialization.getClass().getName());

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(outputStream);
            context.write(out);
            out.flush();

            writer.write(outputStream.toByteArray());
            writer.flush();

            notWriteSerdeContext = false;
        }
    }


    public void write(ByteBuffer message) throws IOException {
        if(notWriteSerdeContext) {
            writeSerdeContext();
        }
        writer.write(message);
    }

    public void flush() throws IOException {
        writer.flush();
    }

    public void close() throws IOException {
        writer.close();
    }


    public static void main(String[] args) {
        Iterator iterator = ServiceLoader.load(Serialization.class).iterator();
        Serialization serialization = (Serialization)iterator.next();

        LOG.info("Serialization class: " + serialization.getClass().getName());
        System.out.println(serialization.getContentType());

    }
}
