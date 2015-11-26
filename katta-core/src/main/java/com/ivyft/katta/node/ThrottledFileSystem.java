package com.ivyft.katta.node;

import com.ivyft.katta.util.ThrottledInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-19
 * Time: 下午2:01
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class ThrottledFileSystem  extends FileSystem {

    /**
     * 文件系统，可以模拟更多，如S3等
     */
    private final FileSystem fileSystemDelegate;


    /**
     * 从文件系统的输入流，模拟其他支持的文件系统
     */
    private final ThrottledInputStream.ThrottleSemaphore throttleSemaphore;


    /**
     *
     * @param fileSystem
     * @param throttleSemaphore
     */
    public ThrottledFileSystem(FileSystem fileSystem, ThrottledInputStream.ThrottleSemaphore throttleSemaphore) {
        this.fileSystemDelegate = fileSystem;
        this.throttleSemaphore = throttleSemaphore;
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
        return this.fileSystemDelegate.append(path, bufferSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path path,
                                     FsPermission permission,
                                     boolean overwrite,
                                     int bufferSize,
                                     short replication,
                                     long blockSize,
                                     Progressable progress) throws IOException {
        return this.fileSystemDelegate.create(path, permission,
                overwrite, bufferSize,
                replication, blockSize, progress);
    }

    @Override
    public boolean delete(Path path) throws IOException {
        return this.fileSystemDelegate.delete(path, true);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return this.fileSystemDelegate.delete(path, recursive);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return this.fileSystemDelegate.getFileStatus(path);
    }

    @Override
    public URI getUri() {
        return this.fileSystemDelegate.getUri();
    }

    @Override
    public Path getWorkingDirectory() {
        return this.fileSystemDelegate.getWorkingDirectory();
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return this.fileSystemDelegate.listStatus(path);
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        return this.fileSystemDelegate.mkdirs(path, permission);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        ThrottledInputStream throttledInputStream = new ThrottledInputStream(
                this.fileSystemDelegate.open(path, bufferSize),
                this.throttleSemaphore);
        return new FSDataInputStream(throttledInputStream);
    }

    @Override
    public boolean rename(Path path, Path dist) throws IOException {
        return this.fileSystemDelegate.rename(path, dist);
    }

    @Override
    public void setWorkingDirectory(Path path) {
        this.fileSystemDelegate.setWorkingDirectory(path);
    }

    @Override
    public Configuration getConf() {
        return this.fileSystemDelegate.getConf();
    }
}
