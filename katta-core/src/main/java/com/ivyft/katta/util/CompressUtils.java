package com.ivyft.katta.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/11
 * Time: 16:33
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class CompressUtils {


    private static Logger LOG = LoggerFactory.getLogger(CompressUtils.class);


    /**
     * 使用文件扩展名来推断二来的codec来对文件进行解压缩
     *
     * @param zipFile
     * @param fs
     * @param uncompressPath
     * @throws IOException
     */
    public static void uncompressZip(Path zipFile, FileSystem fs, File uncompressPath)
            throws IOException {

        // 如果指定的路径不存在,创建文件夹
        if (!uncompressPath.exists() || !uncompressPath.isDirectory()) {
            if (!uncompressPath.mkdirs()) {
                StringBuilder exception = new StringBuilder();
                exception.append(uncompressPath);
                exception.append("路径不可到达,并且解压缩");
                exception.append(zipFile);
                exception.append("失败!");
                throw new IOException(exception.toString());
            }
        }

        Path dst = new Path(System.getProperty("java.io.tmpdir") + File.separator + System.currentTimeMillis() + "-" + zipFile.getName());
        fs.copyToLocalFile(zipFile, dst);

        ZipFileOperator zipFileOperator = new ZipFileOperator();
        zipFileOperator.uncompressZip(new File(dst.toString()), uncompressPath);

    }


    public static void main(String[] args) throws IOException {
        String property = System.getProperty("java.io.tmpdir");
        System.out.println(property);
        CompressUtils.uncompressZip(new Path("/lib/solr/solr.zip"),
                FileSystem.get(new Configuration()),
                new File("./data/uncomp", "solr"));
    }
}
