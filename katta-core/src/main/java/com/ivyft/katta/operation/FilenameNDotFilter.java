package com.ivyft.katta.operation;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

/**
 *
 * <p>
 *     排除前缀是.的文件及文件夹
 * </p>
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-11-20
 * Time: 下午1:37
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public class FilenameNDotFilter implements PathFilter, FilenameFilter {



    private final FileSystem fs;



    private boolean onlyDir = false;

    public FilenameNDotFilter() {
        this(null, false);
    }


    public FilenameNDotFilter(FileSystem fs, boolean onlyDir) {
        this.fs = fs;
        this.onlyDir = onlyDir;
    }


    /**
     * Tests whether or not the specified abstract pathname should be
     * included in a pathname list.
     *
     * @param path The abstract pathname to be tested
     * @return <code>true</code> if and only if <code>pathname</code>
     *         should be included
     */
    @Override
    public boolean accept(Path path) {
        try {
            return onlyDir ?
                    (fs.isDirectory(path) && !path.getName().startsWith(".")) :
                    !path.getName().startsWith(".");
        } catch (IOException e) {
            return false;
        }
    }


    @Override
    public boolean accept(final File dir, final String name) {
        return onlyDir ? (dir.isDirectory() && !name.startsWith(".")) :
                !name.startsWith(".");
    }


    public FileSystem getFs() {
        return fs;
    }

    public boolean isOnlyDir() {
        return onlyDir;
    }


    public void setOnlyDir(boolean onlyDir) {
        this.onlyDir = onlyDir;
    }
}
