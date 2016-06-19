package com.ivyft.katta.lib.writer;

import com.ivyft.katta.codec.Serializer;
import com.ivyft.katta.node.LuceneIndexMergeManager;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/3/28
 * Time: 15:31
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class DefaultLuceneDocumentMerger extends LuceneDocumentMerger {


    public DefaultLuceneDocumentMerger(Serializer<Object> serializer,
                                       Analyzer analyzer,
                                       File tempIndexLocalPath,
                                       DocumentFactory documentFactory,
                                       LuceneIndexMergeManager mergeManager) {
        super(serializer, analyzer, tempIndexLocalPath, documentFactory, mergeManager);
    }

    @Override
    public int addDoc(Collection<Document> docs) {
        if(docs == null) {
            return 0;
        }
        for (Document doc : docs) {
            try {
                int delete = mergeManager.delete("word", doc.get("word"));
                if(delete > 0) {
                     indexWriter.addDocument(doc);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return docs.size();
    }
}
