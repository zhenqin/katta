/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ivyft.katta.node;

import com.ivyft.katta.util.FileUtil;
import com.ivyft.katta.util.HadoopUtil;
import com.ivyft.katta.util.KattaException;
import com.ivyft.katta.util.ThrottledInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;


/**
 *
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-13
 * Time: 上午8:58
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class ShardManager {


    /**
     * 索引文件复制的本地路径
     */
    private final File shardsFolder;


    /**
     * 复制数据的文件系统
     */
    private final ThrottledInputStream.ThrottleSemaphore throttleSemaphore;


    /**
     * Log
     */
    protected final static Logger LOG = LoggerFactory.getLogger(ShardManager.class);


    /**
     *
     * @param shardsFolder
     */
    public ShardManager(File shardsFolder) {
        this(shardsFolder, null);
    }


    /**
     *
     * @param shardsFolder
     * @param throttleSemaphore
     */
    public ShardManager(File shardsFolder, ThrottledInputStream.ThrottleSemaphore throttleSemaphore) {
        this.shardsFolder = shardsFolder;
        this.throttleSemaphore = throttleSemaphore;
        if (!this.shardsFolder.exists()) {
            this.shardsFolder.mkdirs();
        }
        if (!this.shardsFolder.exists()) {
            throw new IllegalStateException("could not create local shard folder '" +
                    this.shardsFolder.getAbsolutePath() + "'");
        }
    }


    /**
     * 安装Shard，提供一个ShardName和一个Path
     * @param shardName shardName
     * @param shardPath IndexFile Path
     * @return 返回本地索引文件的路径
     * @throws Exception
     */
    public File installShard(String shardName,
                             String shardPath) throws Exception {
        File localShardFolder = getShardFolder(shardName);
        try {
            if (!localShardFolder.exists()) {
                installShard(shardName, shardPath, localShardFolder);
            }
            return localShardFolder;
        } catch (Exception e) {
            FileUtil.deleteFolder(localShardFolder);
            throw e;
        }
    }


    /**
     * 移除shardName的shard
     * @param shardName shardName
     */
    public void uninstallShard(String shardName) {
        File localShardFolder = getShardFolder(shardName);
        try {
            if (localShardFolder.exists()) {
                FileUtils.deleteDirectory(localShardFolder);
            }
        } catch (IOException e) {
            throw new RuntimeException("could not delete folder '" + localShardFolder + "'");
        }
    }


    /**
     *
     * 返回本地所有Shard索引的路径
     *
     * @return 返回所有shardName
     */
    public Collection<String> getInstalledShards() {
        String[] folderList = this.shardsFolder.list(FileUtil.VISIBLE_FILES_FILTER);
        if (folderList == null || folderList.length == 0) {
            return Collections.EMPTY_LIST;
        }
        return Arrays.asList(folderList);
    }


    /**
     * 本地存放Shard Index路径
     * @return
     */
    public File getShardsFolder() {
        return this.shardsFolder;
    }


    /**
     * 拼接本地Shard路径
     * @param shardName shardName
     * @return
     */
    public File getShardFolder(String shardName) {
        return new File(this.shardsFolder, shardName);
    }



    /**
     * 檢驗本地Shard是否需要重新安裝。本地文件存在否？修改時間是否匹配
     * @param shardName shardName Shard Name
     * @param shardPath Shard在遠程路徑
     * @return 返回true則重新安裝
     */
    public boolean validChanges(String shardName, String shardPath) {
        File localShardFolder = getShardFolder(shardName);
        URI uri;
        try {
            uri = new URI(shardPath);
            FileSystem fileSystem = HadoopUtil.getFileSystem(uri);
            if (this.throttleSemaphore != null) {
                fileSystem =
                        new ThrottledFileSystem(fileSystem, this.throttleSemaphore);
            }
            final Path path = new Path(shardPath);
            //Shard原索引是否還存在
            if(!fileSystem.exists(path)) {
                LOG.info("server index was delete, shard path: " + shardPath);
                //Shard遠程索引被刪除了，不需要重新安裝
                return false;
            }

            //本地索引不存在了，重新安装
            if (!localShardFolder.exists()) {
                LOG.info("local index was delete, will install agent, shard path: " + shardPath);
                return true;
            }

            //boolean isZip = fileSystem.isFile(path) && shardPath.endsWith(".zip");
            //zip就算了，不要解压了。如果是文件夹，需要比对一下日期
            //日期是否相同？大小是否相同（文件夹不能比大小）？
            long l = fileSystem.getFileStatus(path).getModificationTime();
            long l2 = localShardFolder.lastModified();
            LOG.info("local by server diff " + (l2 - l));

            //两个索引时间相差在10分钟之内, 不需要重新安装
            return Math.abs(l2 - l) >= 10 * 60 * 1000L;
        } catch (Exception e) {
            LOG.warn(ExceptionUtils.getFullStackTrace(e));
            return true;
        }
    }

    /*
     * Loads a shard from the given URI. The uri is handled bye the hadoop file
     * system. So all hadoop support file systems can be used, like local hdfs s3
     * etc. In case the shard is compressed we also unzip the content. If the
     * system property katta.spool.zip.shards is true, the zip file is staged to
     * the local disk before being unzipped.
     */
    private void installShard(String shardName,
                              String shardPath,
                              File localShardFolder) throws KattaException {
        LOG.info("install shard '" + shardName + "' from " + shardPath);
        URI uri = null;
        FileSystem fileSystem = null;
        try {
            uri = new URI(shardPath);
            fileSystem = FileSystem.get(uri, HadoopUtil.getHadoopConf());
            if (this.throttleSemaphore != null) {
                fileSystem =
                        new ThrottledFileSystem(fileSystem, this.throttleSemaphore);
            }
        } catch (URISyntaxException e) {
            throw new KattaException("Can not parse uri for path: " + shardPath, e);
        } catch (Exception e) {
            throw new KattaException("Can not init file system " + uri, e);
        }
        int maxTries = 5;
        for (int i = 0; i < maxTries; i++) {
            try {
                //原路径
                final Path path = new Path(shardPath);
                boolean isZip = fileSystem.isFile(path) && shardPath.endsWith(".zip");

                File shardTmpFolder = new File(localShardFolder.getAbsolutePath() + "_tmp");

                //删除旧的
                FileUtil.deleteFolder(shardTmpFolder);
                LOG.info("delete folder: " + shardTmpFolder.getAbsolutePath());

                if (isZip) {
                    FileUtil.unzip(path, shardTmpFolder, fileSystem, System.getProperty("katta.spool.zip.shards", "false")
                            .equalsIgnoreCase("true"));
                } else {
                    fileSystem.copyToLocalFile(false, path, new Path(shardTmpFolder.getAbsolutePath()), true);
                    LOG.info("copy shard to local: " + shardTmpFolder.getAbsolutePath());
                }

                boolean b = shardTmpFolder.renameTo(localShardFolder);
                LOG.info(shardTmpFolder.getName() + " rename to " + localShardFolder.getAbsolutePath() + " " + b);

                //文件修改時間
                long lastModified = fileSystem.getFileStatus(path).getModificationTime();
                b = localShardFolder.setLastModified(lastModified);
                LOG.info(localShardFolder.getAbsolutePath() + " lastModifies " + new Date(lastModified) + " " + b);

                // Looks like we were successful.
                if (i > 0) {
                    LOG.error("Loaded shard:" + shardPath);
                }
                return;
            } catch (Exception e) {
                LOG.error(String.format("Error loading shard: %s (try %d of %d)", shardPath, i, maxTries), e);
                if (i >= maxTries - 1) {
                    throw new KattaException("Can not load shard: " + shardPath, e);
                }
            }
        }
    }
}
