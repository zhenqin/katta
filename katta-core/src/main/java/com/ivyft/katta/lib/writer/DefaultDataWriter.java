package com.ivyft.katta.lib.writer;

import com.ivyft.katta.codec.Serializer;
import com.ivyft.katta.codec.jdkserializer.JdkSerializer;
import com.ivyft.katta.protocol.IntLengthHeaderFile;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.HadoopUtil;
import com.ivyft.katta.util.MasterConfiguration;
import com.ivyft.katta.util.StringHash;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/29
 * Time: 11:55
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class DefaultDataWriter extends DataWriter {

    Serializer serializer = new JdkSerializer();


    private int shardNum = 3;


    private int step;


    private int numPartitions;


    protected Map<Integer, IntLengthHeaderFile.Writer> writerMap = new HashMap<Integer, IntLengthHeaderFile.Writer>();

    public DefaultDataWriter() {

    }

    @Override
    public void init(MasterConfiguration conf, InteractionProtocol protocol) {
        this.step = conf.getInt("master.data.shard.step", 5);

        this.numPartitions = shardNum * step;
    }


    public IntLengthHeaderFile.Writer get(String shardId) {
        int hashCode = StringHash.murmurhash3_x86_32(shardId, 0, shardId.length(), 0);
        int microShard = Math.abs(hashCode % shardNum);
        IntLengthHeaderFile.Writer writer = writerMap.get(microShard);
        if(writer == null) {
            try {
                FileSystem fileSystem = HadoopUtil.getFileSystem(new URI("file:///Users"));
                Path parent = new Path("./data/serde/" + microShard);
                fileSystem.mkdirs(parent);
                writer = new IntLengthHeaderFile.Writer(
                        fileSystem, new Path(parent, "mydata.dat"));

                String info = "start=" + (microShard * step) + "\n" +
                        "end=" + (microShard * step + step);

                FSDataOutputStream out = fileSystem.create(new Path(parent, "info.properties"), true);
                IOUtils.write(info, out);
                IOUtils.closeQuietly(out);

                writerMap.put(microShard, writer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return writer;
    }



    @Override
    public void write(String shardId, ByteBuffer objByte) {
        Object deserialize = serializer.deserialize(objByte.array());
        System.out.println(shardId + "    " + deserialize);
        try {
            get(shardId).write(objByte);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        for (IntLengthHeaderFile.Writer writer : writerMap.values()) {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
