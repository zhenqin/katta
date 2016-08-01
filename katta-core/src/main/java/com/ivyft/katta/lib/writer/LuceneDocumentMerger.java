package com.ivyft.katta.lib.writer;

import com.ivyft.katta.codec.Serializer;
import com.ivyft.katta.node.LuceneIndexMergeManager;
import com.ivyft.katta.util.FileUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/3/25
 * Time: 18:18
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public abstract class LuceneDocumentMerger {


    /**
     * 对象序列化器
     */
    protected final Serializer<Object> serializer;


    /**
     * 临时索引地址
     */
    protected final File path;


    /**
     * Lucene Document
     */
    protected final DocumentFactory documentFactory;


    /**
     * Lucene 主索引管理
     */
    protected final LuceneIndexMergeManager mergeManager;


    /**
     * 本地临时索引的 IndexWriter
     */
    protected final IndexWriter indexWriter;




    static Logger LOG = LoggerFactory.getLogger(LuceneDocumentMerger.class);




    public LuceneDocumentMerger(Serializer<Object> serializer,
                                Analyzer analyzer,
                                File tempIndexLocalPath,
                                DocumentFactory documentFactory,
                                LuceneIndexMergeManager mergeManager) {
        this.serializer = serializer;
        this.mergeManager = mergeManager;
        this.path = tempIndexLocalPath;
        this.documentFactory = documentFactory;

        try {
            indexWriter = new IndexWriter(FSDirectory.open(tempIndexLocalPath), new IndexWriterConfig(Version.LUCENE_46, analyzer));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

    }


    /**
     * 添加一条记录
     * @param buffer 对象字节
     * @return 返回影响的行数
     */
    public int add(ByteBuffer buffer) {
        Collection<Document> docs = documentFactory.get(serializer.deserialize(buffer.array()));
        return addDoc(docs);
    }


    /**
     *
     * @param docs Lucene Document
     * @return 返回影响的行数
     */
    public abstract int addDoc(Collection<Document> docs);


    /**
     *
     * 合并索引，Lucene Server 在提交数据更新时，把数据反序列化为 Document，并 add 到 Index 中，后需要调用 merge 方法。
     *
     * @throws IOException 合并索引时发生异常
     */
    public void merge() throws IOException {
        try {
            int addDocs = indexWriter.numDocs();
            indexWriter.commit();
            indexWriter.close(true);

            LOG.info("merge max docs {}", addDocs);
            //mergeManager.mergeIndex(path);
            mergeManager.commitAndDelete();
            mergeManager.optimize(5);
        } finally {
            try {
                mergeManager.close();
            } catch (Exception e) {
                LOG.warn(e.getMessage());
            }
            try {
                LOG.info("clean folder: " + path.getAbsolutePath());
                FileUtil.deleteFolder(path);
            } catch (Exception e) {

            }
        }
    }
}
