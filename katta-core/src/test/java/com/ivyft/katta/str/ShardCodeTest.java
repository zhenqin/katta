package com.ivyft.katta.str;

import com.ivyft.katta.codec.Serializer;
import com.ivyft.katta.codec.jdkserializer.JdkSerializer;
import com.ivyft.katta.protocol.IntLengthHeaderFile;
import com.ivyft.katta.util.HadoopUtil;
import com.ivyft.katta.util.StringHash;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/29
 * Time: 10:52
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class ShardCodeTest {

    public ShardCodeTest() {
    }


    @Test
    public void testClentRun() throws Exception {
        IntLengthHeaderFile.Reader reader = new IntLengthHeaderFile.Reader(
                        HadoopUtil.getFileSystem(new URI("file:///Users")), new Path("file:///Volumes/Study/IntelliJ/tech/katta1/data/serde/mydata.dat"));

        Serializer serializer = new JdkSerializer();
        while (reader.hasNext()) {
            System.out.println(serializer.deserialize(reader.next()));
        }

        reader.close();

    }

    @Test
    public void testCode() throws Exception {
        int numPartitions = 3;
        //int shards = 10;
        //int rootShard = 10;

        List<String> srcs = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            srcs.add(DigestUtils.md5Hex(String.valueOf(new Random().nextInt(1000000000))));
            //srcs.add("adfasdfhjlakjhdsflakjshdflk");
        }

        for (String keyStr : srcs) {

            int hashCode = StringHash.murmurhash3_x86_32(keyStr, 0, keyStr.length(), 0);
            int microShard = Math.abs(hashCode % numPartitions);


            System.out.println(microShard);
        }
    }
}
