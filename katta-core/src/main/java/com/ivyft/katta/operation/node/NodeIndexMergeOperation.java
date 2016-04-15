package com.ivyft.katta.operation.node;

import com.ivyft.katta.lib.writer.*;
import com.ivyft.katta.node.NodeContext;
import com.ivyft.katta.node.ShardManager;
import com.ivyft.katta.protocol.IntLengthHeaderFile;
import com.ivyft.katta.util.HadoopUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/3/25
 * Time: 10:57
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class NodeIndexMergeOperation extends AbstractShardOperation {


    /**
     * 序列化
     */
    private static final long serialVersionUID = 1L;



    protected final String nodeName;


    /**
     * Index Name
     */
    protected String indexName;


    /**
     * Commit ID
     */
    protected String commitId;


    /**
     * merge commit
     */
    protected Set<ShardRange> commits;


    /**
     * LOG
     */
    private static Logger log = LoggerFactory.getLogger(NodeIndexMergeOperation.class);

    public NodeIndexMergeOperation(String node, String indexName, String commitId, Set<ShardRange> commits, Set<String> shards) {
        this.nodeName = node;
        this.indexName = indexName;
        this.commitId = commitId;
        this.commits = commits;

        for (String shard : shards) {
            addShard(shard);
        }
    }

    @Override
    protected String getOperationName() {
        return "merge-index";
    }

    @Override
    protected void execute(NodeContext context, String shardName, DeployResult result) throws Exception {
        //String shardPath = getShardPath(shardName);
        log.info("merge lucene index. index name {} shard {}", indexName, shardName);
        ShardManager shardManager = context.getShardManager();
        LuceneDocumentMerger luceneDocumentMerger = null;
        AtomicLong addCount = new AtomicLong(0);

        for (ShardRange commit : commits) {

            log.info("start merge commit {} path {}", commit.getShardName(), commit.getShardPath());
            Path shardPath = new Path(commit.getShardPath());
            List<Path> paths = shardManager.getDataPaths(shardPath);
            for (Path path : paths) {
                IntLengthHeaderFile.Reader reader = new IntLengthHeaderFile.Reader(HadoopUtil.getFileSystem(), path);
                try {
                    SerializationReader r = new SerializationReader(reader);
                    SerdeContext serdeContext = r.getSerdeContext();
                    log.info("serde context {}", serdeContext.toString());

                    Class<Serialization> aClass = (Class<Serialization>) Class.forName(serdeContext.getSerClass());
                    Serialization serialization = aClass.newInstance();
                    if(luceneDocumentMerger == null) {
                        SerialFactory.registry(serialization);
                        luceneDocumentMerger = shardManager.getMergeDocument(serialization.getContentType(), indexName, shardName);
                    }
                    int count = 0;
                    ByteBuffer byteBuffer = r.nextByteBuffer();
                    while (byteBuffer != null) {
                        int add = luceneDocumentMerger.add(byteBuffer);
                        if(add > 0) {
                            addCount.addAndGet(add);
                        }
                        count++;

                        byteBuffer = r.nextByteBuffer();

                        if(count % 1000 == 0) {
                            log.info("add lucene document count {}", count);
                        }
                    }

                    log.info("add last lucene document count {}", count);
                } finally {
                    reader.close();
                }
            }

            //end shard commit

//            */

        }

        Map<String, String> meta = new HashMap<String, String>(3);
        meta.put("addCount", String.valueOf(addCount.get()));
        meta.put("shardName", shardName);
        meta.put("indexName", this.getIndexName());
        meta.put("nodeName", this.nodeName);
        meta.put("commitId", this.getCommitId());

        result.addShardMetaDataMap(shardName, meta);

        try {
            luceneDocumentMerger.merge();
        } catch (Exception e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
        }
        log.info("merge index success");
        log.info("index {} commitid {}", indexName, commitId);
    }

    @Override
    protected void onException(NodeContext context, String shardName, Exception e) {

    }


    public String getIndexName() {
        return indexName;
    }

    public String getCommitId() {
        return commitId;
    }

    public Set<ShardRange> getCommits() {
        return commits;
    }
}
