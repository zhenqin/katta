package com.ivyft.katta.lib.writer;

import com.ivyft.katta.codec.Serializer;
import com.ivyft.katta.node.LuceneIndexMergeManager;
import com.ivyft.katta.util.FileUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
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
public class MergeDocument {

    Serializer<Object> serializer;
    File path;


    DocumentFactory documentFactory;

    LuceneIndexMergeManager mergeManager;



    static Logger LOG = LoggerFactory.getLogger(MergeDocument.class);




    public MergeDocument(Serializer<Object> serializer, Analyzer analyzer, File path, DocumentFactory documentFactory, LuceneIndexMergeManager mergeManager) {
        this.serializer = serializer;
        this.mergeManager = mergeManager;
        this.path = path;
        this.documentFactory = documentFactory;
    }



    public void add(ByteBuffer buffer) {
        Object deserialize = serializer.deserialize(buffer.array());
        Collection<Document> docs = documentFactory.get(deserialize);

        /*
        for (Document doc : docs) {

        }
        */
        LOG.info(deserialize.toString());
    }



    public void merge() throws IOException {
        try {
            //mergeManager.mergeIndex(path);
            //mergeManager.optimize(5);
            mergeManager.close();
        } finally {
            //mergeManager.close();
            try {
                LOG.info("clean folder: " + path.getAbsolutePath());
                FileUtil.deleteFolder(path);
            } catch (Exception e) {

            }
        }
    }
}
