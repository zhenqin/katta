package com.ivyft.katta.tool.index;

import com.ivyft.katta.node.IndexUpdateListener;
import com.ivyft.katta.node.LuceneIndexMergeManager;
import com.ivyft.katta.node.ShardManager;
import com.ivyft.katta.util.NodeConfiguration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 2017/12/28
 * Time: 13:30
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class IndexMergeTool {


    /**
     * Log
     */
    protected final static Logger LOG = LoggerFactory.getLogger(IndexMergeTool.class);



    private final IndexUpdateListener updateListener;




    /**
     *
     * Copy Index to Local
     */
    protected final boolean copyIndexToLocal;




    /**
     * Merge Index Analyzer
     */
    protected String analyzerClass;


    /**
     *
     * @param conf
     * @param updateListener
     */
    public IndexMergeTool(NodeConfiguration conf, IndexUpdateListener updateListener) {
        this.updateListener = updateListener;

        setAnalyzerClass(conf.getString("lucene.index.writer.analyzer.class", StandardAnalyzer.class.getName()));
        this.copyIndexToLocal = conf.getBoolean("lucene.index.copyTo.local", true);
    }



    public String getAnalyzerClass() {
        return analyzerClass;
    }

    public void setAnalyzerClass(String analyzerClass) {
        try {
            Class.forName(analyzerClass);
            this.analyzerClass = analyzerClass;
            LOG.info("analyzer class {}", analyzerClass);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }


    /**
     * 把 2 个以上的索引合并到第一个，使用第一个的地址。 该方法支持 HDFS 和 File
     * @param segment 索引段数量
     * @param paths
     * @throws IOException
     */
    public void mergeIndex(int  segment, boolean mergedDelete, URI... paths) throws IOException {
        mergeIndex(true, segment, mergedDelete, paths);
    }


    /**
     * 把 2 个以上的索引合并到第一个，使用第一个的地址。 该方法支持 HDFS 和 File
     * @param segment 索引段数量
     * @param paths
     * @throws IOException
     */
    public void mergeIndex(boolean optimize, int segment, boolean mergedDelete, URI... paths) throws IOException {
        if(paths.length < 1) {
            throw new IllegalArgumentException("paths value index path greater than 1 or more.");
        }

        // 最少一个段方式，内部文件数过多造成进程持有的文件句柄数过多
        if(segment <= 0) {
            segment = 1;
        }

        if(paths.length == 1) {
            if(!optimize) {
                throw new IllegalArgumentException("one index path can not merge, set optimize = true.");
            }
            // 仅仅是优化索引
            LOG.info("optimize {} to {} segment.", paths[0], segment);

            File indexPath = new File(paths[0].getPath());
            LuceneIndexMergeManager manager = null;
            try {
                manager = new LuceneIndexMergeManager(indexPath.toURI(),
                        indexPath.getName(), indexPath.getName(),
                        updateListener, ShardManager.getAnalyzer((Class<? extends Analyzer>) Class.forName(getAnalyzerClass())));
                manager.optimize(segment);
            } catch (ClassNotFoundException e){
                throw new IOException("无法加载类", e);
            } finally {
                if(manager != null) {
                    manager.close();
                }
            }

            return;
        }

        URI uri = paths[0];
        List<URI> shards = new ArrayList<>(paths.length - 1);
        for (int i = 1; i < paths.length; i++) {
            shards.add(paths[i]);
        }
        LOG.info("merge index '" + shards + "' to " + uri);
        File indexPath = new File(paths[0].getPath());
        LuceneIndexMergeManager manager = null;
        try {
            manager = new LuceneIndexMergeManager(indexPath.toURI(),
                    indexPath.getName(), indexPath.getName(),
                    updateListener, ShardManager.getAnalyzer((Class<? extends Analyzer>) Class.forName(getAnalyzerClass())));

            for (URI shard : shards) {
                manager.mergeIndex(shard, mergedDelete);
            }

            if(optimize) {
                manager.optimize(segment);
            }
        } catch (ClassNotFoundException e){
            throw new IOException("无法加载类", e);
        } finally {
            if(manager != null) {
                manager.close();
            }
        }
    }


    public static void main(String[] args) throws Exception {
        IndexMergeTool tool = new IndexMergeTool(new NodeConfiguration(), new IndexUpdateListener() {
            @Override
            public void onBeforeUpdate(String indexName, String shardName) {

            }

            @Override
            public void onAfterUpdate(String indexName, String shardName) {
                LOG.info("索引将要关闭。");
            }
        });


        tool.mergeIndex(1, true,
                new URI("file:///Volumes/Study/IntelliJ/yiidata/katta1/data/lucene/P2D95Ggl2tWnSynu8Xg"),
                new URI("file:///Volumes/Study/IntelliJ/yiidata/katta1/data/katta-shards/zhenqin-pro102_20000/userindex%232PD95Ggl2tWnSynu8gX"));
    }

}
