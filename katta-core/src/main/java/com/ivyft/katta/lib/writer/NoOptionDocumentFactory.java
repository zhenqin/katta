package com.ivyft.katta.lib.writer;

import com.ivyft.katta.protocol.IntLengthHeaderFile;
import com.ivyft.katta.util.HadoopUtil;
import com.ivyft.katta.util.NodeConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/6
 * Time: 14:11
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class NoOptionDocumentFactory<T> extends AbsDocumentFactory<T> {


    @Override
    public Collection<Document> get(T obj) {
        if(obj instanceof String) {
            Document document = new Document();
            document.add(new StringField("word", (String)obj, Field.Store.YES));
            return Arrays.asList(document);
        }
        return null;
    }


    @Override
    public void init(NodeConfiguration conf) {

    }


    public static void main(String[] args) throws IOException {
        Path p = new Path("/user/katta/data/hello/QLnNuUhgC9Mf4q0p74C/data/4618cc4f9d07-151220154032-data.dat");
        IntLengthHeaderFile.Reader reader = new IntLengthHeaderFile.Reader(HadoopUtil.getFileSystem(p), p);
        SerializationReader r = new SerializationReader(reader);

        DocumentFactory documentFactory = new NoOptionDocumentFactory();
        System.out.println(r.getSerdeContext());
        while (true) {
            ByteBuffer buffer = null;
            try {
                buffer = r.nextByteBuffer();
            } catch (IOException e) {
                return;
            }
            //Collection list = documentFactory.deserial(r.getSerdeContext(), buffer);
            //documentFactory.get(list);
        }

    }
}
