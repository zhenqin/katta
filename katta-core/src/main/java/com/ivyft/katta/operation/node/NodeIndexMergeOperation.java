package com.ivyft.katta.operation.node;

import com.ivyft.katta.codec.Serializer;
import com.ivyft.katta.lib.writer.*;
import com.ivyft.katta.node.NodeContext;
import com.ivyft.katta.node.ShardManager;
import com.ivyft.katta.protocol.IntLengthHeaderFile;
import com.ivyft.katta.util.HadoopUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

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

    public NodeIndexMergeOperation() {
    }

    public NodeIndexMergeOperation(String indexName, String commitId, Set<ShardRange> commits) {
        this.indexName = indexName;
        this.commitId = commitId;
        this.commits = commits;

        for (ShardRange commit : commits) {
            this.addShard(commit.getShardName());
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
        MergeDocument mergeDocument = null;
        for (ShardRange commit : commits) {
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
                    if(mergeDocument == null) {
                        SerialFactory.registry(serialization);
                        mergeDocument = shardManager.getMergeDocument(serialization.getContentType(), indexName, shardName);
                    }
                    int count = 0;
                    ByteBuffer byteBuffer = r.nextByteBuffer();
                    while (byteBuffer != null) {
                        mergeDocument.add(byteBuffer);
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

//            */

        }

        mergeDocument.merge();
        log.info("merge index success");

        //IndexWriter indexWriter = shardManager.getShardIndexWriter(shardName, shardPath);
        //URI localShardFolder = context.getShardManager().installShard(shardName, shardPath);
        //log.info("copy shard " + shardName + " success. local: " + localShardFolder);

        //IContentServer contentServer = context.getContentServer();


        log.info("index {} commitid {}", indexName, commitId);
    }

    @Override
    protected void onException(NodeContext context, String shardName, Exception e) {

    }
}
