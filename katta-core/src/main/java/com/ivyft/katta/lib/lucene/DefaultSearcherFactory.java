/**
 * Copyright 2011 the original author or authors.
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
package com.ivyft.katta.lib.lucene;

import com.ivyft.katta.node.ShardManager;
import com.ivyft.katta.util.HadoopUtil;
import com.ivyft.katta.util.NodeConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;


/**
 * <p>
 *
 * 该类是一个工厂方法。 用于根据Index dir生成IndexSearcher。 提供搜索
 * </p>
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
public class DefaultSearcherFactory implements ISeacherFactory {


    protected static Logger LOG = LoggerFactory.getLogger(DefaultSearcherFactory.class);

    /**
     * 该类通过反射实例化， 因此必须有无参数的构造方法
     */
    public DefaultSearcherFactory() {

    }

    @Override
    public void init(NodeConfiguration config) {
        // nothing to do
    }

    @Override
    public IndexSearcher createSearcher(String shardName, URI shardPath) throws IOException {
        String scheme = shardPath.getScheme();
        if(StringUtils.equals(ShardManager.HDFS, scheme)) {
            LOG.info("open hdfs index: " + shardPath.toString());
            Configuration hadoopConf = HadoopUtil.getHadoopConf();
            Directory directory = new HdfsDirectory(new Path(shardPath), hadoopConf);
            return new IndexSearcher(DirectoryReader.open(directory));
        } else if(StringUtils.equals(ShardManager.FILE, scheme)) {
            LOG.info("open file index: " + shardPath.toString());
            FSDirectory directory = FSDirectory.open(new File(shardPath));
            return new IndexSearcher(DirectoryReader.open(directory));
        } else {
            throw new IllegalStateException("unknow schema " + scheme + " and path: " + shardPath.toString());
        }
    }


    public static void main(String[] args) {
        Path path = new Path("hdfs:///user");
        System.out.println(path.toUri().getScheme());

        path = new Path("file:///user");
        System.out.println(path.toUri().getScheme());
    }

}
