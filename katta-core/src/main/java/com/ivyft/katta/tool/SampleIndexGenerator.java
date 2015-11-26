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

package com.ivyft.katta.tool;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

/**
 * Generates a test index, for example used for benchmarking
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
public class SampleIndexGenerator {

    public void createIndex(String input, String output, int wordsPerDoc, int indexSize) {
        createIndex(getWordList(input), output, wordsPerDoc, indexSize);
    }

    public void createIndex(String[] wordList, String output, int wordsPerDoc, int indexSize) {
        long startTime = System.currentTimeMillis();
        String hostname = "unknown";
        InetAddress addr;
        try {
            addr = InetAddress.getLocalHost();
            hostname = addr.getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Unable to get localhostname", e);
        }

        File index = new File(output, hostname + "-" + UUID.randomUUID().toString());
        int count = wordList.length;
        Random random = new Random(System.currentTimeMillis());
        try {
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_46,
                    new StandardAnalyzer(Version.LUCENE_46));
            TieredMergePolicy mergePolicy = new TieredMergePolicy();

            //如果索引的段超过这个值,则永远不会被合并,默认5G, 这里2G
            mergePolicy.setMaxMergedSegmentMB(2 * 1024);

            //当索引段超过这个值, 则进行合并索引操作
            mergePolicy.setMaxMergeAtOnceExplicit(10);

            //当索引段尺寸超过这个值,则合并. 默认2M
            mergePolicy.setFloorSegmentMB(10);

            //当add的文档超过该值, 则刷新索引, 注意,他不是commit,只是把内存的数据刷写到磁盘
            indexWriterConfig.setMaxBufferedDocs(500);
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

            //当add的文档在内润超过该值,则强制刷新索引, 如:setMaxBufferedDocs
            //这个方法一般用于比较大的文档.
            //该值和setMaxBufferedDocs一起起作用.达到任何一个要求则进行刷写
            indexWriterConfig.setRAMBufferSizeMB(32);

            //当删除文档数超过这个值,立即刷写. 默认-1(禁用这个功能)
            indexWriterConfig.setMaxBufferedDeleteTerms(-1);

            indexWriterConfig.setMergePolicy(mergePolicy);

            IndexWriter indexWriter = new IndexWriter(FSDirectory.open(index), indexWriterConfig);
            for (int i = 0; i < indexSize; i++) {
                // generate text first
                StringBuffer text = new StringBuffer();
                for (int j = 0; j < wordsPerDoc; j++) {
                    text.append(wordList[random.nextInt(count)]);
                    text.append(" ");
                }

                Document document = new Document();
                document.add(new StringField("key", hostname + "_" + i, Store.NO));
                document.add(new TextField("text", text.toString(), Store.NO));
                indexWriter.addDocument(document);

            }
            indexWriter.forceMerge(1, true);
            indexWriter.close();
            System.out.println("Index created with : " + indexSize + " documents in "
                    + (System.currentTimeMillis() - startTime) + " ms");

            // when we are ready we move the index to the final destination and write
            // a done flag file we can use in shell scripts to identify the move is
            // done.

            new File(index, "done").createNewFile();

        } catch (Exception e) {
            throw new RuntimeException("Unable to write index", e);
        }
    }

    /**
     * creates a disctionary of words based on the input text.
     *
     * @throws IOException
     */
    private String[] getWordList(String input) {
        try {
            Set<String> hashSet = new HashSet<String>();
            BufferedReader in = new BufferedReader(new FileReader(input));
            String str;
            while ((str = in.readLine()) != null) {
                String[] words = str.split(" ");
                for (String word : words) {
                    hashSet.add(word);
                }
            }
            in.close();
            return hashSet.toArray(new String[hashSet.size()]);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read sample text", e);
        }
    }
}
