package com.ivyft.katta.node;

import com.ivyft.katta.lib.lucene.SearcherHandle;
import com.ivyft.katta.lib.lucene.SolrHandler;
import com.ivyft.katta.node.dtd.LuceneQuery;
import com.ivyft.katta.node.dtd.LuceneResult;
import com.ivyft.katta.node.dtd.Next;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.*;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-9-29
 * Time: 下午12:48
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public class SocketExportHandler implements Runnable {


    /**
     * 默认的SolrQuery
     */
    public final static SolrQuery DEFAULT_QUERY = new SolrQuery();


    /**
     * Solr Core
     */
    private SolrCore core;


     /**
      * 输入
      */
    private ObjectInputStream inputStream = null;


    /**
     * 输出
     */
    private DataOutputStream outputStream = null;


    /**
     * 查询条件
     */
    private LuceneQuery query;


    /**
     * 对IndexSearcher的引用进行操作计数
     */
    private SearcherHandle searcherHandle;


    /**
     * 获取的SolrSearch, Lucene SearchIndex.
     */
    private IndexSearcher searcher;


    /**
     * 通信的Socket
     */
    private Socket socket;


    /**
     * 连接客户端的IP
     */
    private String ip = null;


    /**
     * 查询结果集
     */
    private TopDocs topDocs;


    /**
     * 开发调试用
     */
    private boolean debug = false;


    /**
     * 日志记录
     */
    private static Logger log = LoggerFactory.getLogger(SocketExportHandler.class);

    /**
     * 构造方法
     * @param socket
     * @param contentServer Solr Core 和 shardName的一个对应
     */
    public SocketExportHandler(Socket socket, IContentServer contentServer) {
        this.socket = socket;

        ip = socket.getInetAddress().getHostAddress();

        try {
            inputStream = new ObjectInputStream(socket.getInputStream());
            outputStream = new DataOutputStream(socket.getOutputStream());
            Object message;

            log.info(ip + " connected success.");

            message = inputStream.readObject();
            if (message == null) {
                throw new IllegalArgumentException("null message.");
            }

            if (message instanceof LuceneQuery) {
                //第一次是查询, 准备好各种参数
                query = (LuceneQuery) message;

                SolrHandler solrHandler = contentServer.getSolrHandlerByShard(query.getShardName());
                if(solrHandler != null) {
                    this.core = solrHandler.getSolrCore();
                }

                if(core == null) {
                    throw new IllegalStateException("找不到：" + query.getShardName()
                            + "对应的Solr Core，取法确认SolrQuery的解析器。");
                }

                //获取Solr 的 Lucene SearchIndex.
                searcherHandle = contentServer.getSearcherHandleByShard(query.getShardName());
                searcher = searcherHandle.getSearcher();

                if(searcher == null) {
                    throw new IllegalStateException("找不到：" + query.getShardName() + "对应的IndexSearcher。");
                }

                DEFAULT_QUERY.set(CommonParams.ROWS, Integer.MAX_VALUE);
                DEFAULT_QUERY.set(CommonParams.DF, core.getLatestSchema().getDefaultSearchFieldName());

                log.info("solr查询默认字段: " + DEFAULT_QUERY.get(CommonParams.DF));
                log.info("export shard name: " + query.getShardName() + " solr query: " + query.getSolrQuery());
            } else {
                throw new IllegalArgumentException("error query Object.");
            }
        } catch (Exception e) {
            log.info(ip + " exception disconnected.");
            if(socket != null) {
                if (inputStream != null && !socket.isClosed() && !socket.isInputShutdown()) {
                    try {
                        inputStream.close();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                }
                if (outputStream != null && !socket.isClosed() && !socket.isOutputShutdown()) {
                    try {
                        outputStream.close();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                }
                if (!socket.isClosed()) {
                    try {
                        socket.close();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                }
            }
            throw new RuntimeException(e);
        } finally {
            if(searcherHandle != null) {
                searcherHandle.finishSearcher();
            }
        }
    }

    /**
     * 当接受到消息, 输出消息
     */
    @Override
    public void run() {
        try {
            //searcher.
            try {
                SolrQuery solrQuery = query.getSolrQuery();

                LocalSolrQueryRequest request = new LocalSolrQueryRequest(core, solrQuery);

                String q = solrQuery.getQuery();
                String[] fq = solrQuery.getFilterQueries();

                boolean b1 = StringUtils.isNotBlank(q);
                boolean b2 = ArrayUtils.isNotEmpty(fq);
                //重新定义一个数组, 把q和qs放到一起
                String[] queryStrings = null;
                if(b1 && b2) {
                    //重新定义一个数组, 把q和qs放到一起
                    queryStrings = new String[fq.length + 1];

                    queryStrings[0] = q;
                    //这里在复制数组, 一定要小心.
                    System.arraycopy(fq, 0, queryStrings, 1, fq.length);
                } else if (b1) {
                    queryStrings = new String[]{q};
                } else if(b2) {
                    queryStrings = fq;
                } else {
                    //q和fq都为null的情况. 直接抛出异常
                    throw new IllegalArgumentException("q or fq must not null.");
                }

                List<Query> queries = SolrPluginUtils.parseQueryStrings(request, queryStrings);

                //把所有的Query用BooleanQuery累积到一起.他们是and的关系
                BooleanQuery booleanQuery = new BooleanQuery();
                for (Query qx : queries) {
                    booleanQuery.add(qx, BooleanClause.Occur.MUST);
                }

                topDocs = searcher.search(booleanQuery, query.getMax());
                log.info("numFount docs:" + topDocs.totalHits);
            } catch (Exception e1) {
                throw new RuntimeException(e1);
            }
            boolean more = true;
            try {
                do {
                    Object message = inputStream.readObject();
                    if (message == null) {
                        continue;
                    }

                    if (message instanceof Next) {
                        Next next = (Next) message;
                        log.info(next.toString());
                        List<SolrDocument> data = new ArrayList<SolrDocument>(next.getLimit());
                        //第二次是循环遍历
                        int i = next.getStart();
                        int offset = next.getStart() + next.getLimit();
                        int maxLength = topDocs.scoreDocs.length;
                        offset = (offset > maxLength) ? maxLength : offset;
                        while (i < offset) {
                            try {
                                Document document;
                                SolrDocument doc = new SolrDocument();
                                //new HashSet<String>(Arrays.asList("sid", "title", "content"));
                                if (query.getFields() == null) {
                                    document = searcher.doc(topDocs.scoreDocs[i].doc);
                                } else {
                                    document = searcher.doc(topDocs.scoreDocs[i].doc, query.getFields());
                                }
                                for (IndexableField field : document) {
                                    if(field.stringValue() == null) {
                                        continue;
                                    }

                                    //判断各种数据类型
                                    if (field.fieldType().docValueType() == FieldInfo.DocValuesType.NUMERIC) {
                                        doc.addField(field.name(), field.numericValue());
                                    } else {
                                        doc.addField(field.name(), field.stringValue());
                                    }
                                }

                                //结果集中是否加入评分? 如果需要加入评分, 则加入
                                if (query.isAddScore()) {
                                    doc.addField("score", topDocs.scoreDocs[i].score);
                                }
                                data.add(doc);
                            } catch (Exception e1) {
                                throw new RuntimeException(e1);
                            }
                            i++;
                        }

                        LuceneResult result = new LuceneResult();
                        result.setDocs(data);
                        result.setLimit(next.getLimit());
                        result.setStart(next.getStart());
                        result.setEnd(offset);
                        result.setTotal(maxLength);
                        result.setMaxScore(topDocs.getMaxScore());

                        if(offset >= maxLength) {
                            more = false;
                        }

                        log.info(ip + " sended, socket output start: [" + next.getStart() + "],end: [" +
                                offset + "], limit: ["
                                + next.getLimit() + "]");

                        result.write(outputStream);
                        //输出给客户端
                        outputStream.flush();
                    }
                } while (more);
                log.info(ip + " will disconnect.");
            } catch (Exception e) {
                log.error(ExceptionUtils.getFullStackTrace(e));
                if (!socket.isConnected() && !socket.isClosed()) {
                    LuceneResult responseEntity = new LuceneResult();
                    responseEntity.setTotal(0);
                    responseEntity.setLimit((short)0);
                    responseEntity.setStart(0);
                    responseEntity.setEnd(0);
                    responseEntity.setMaxScore(0);

                    try {
                        responseEntity.write(outputStream);
                        //输出给客户端
                        outputStream.flush();
                    } catch (Exception e1) {
                        e1.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
        } finally {
            if(searcherHandle != null) {
                searcherHandle.finishSearcher();
            }
            log.info(ip + " disconnected.");
            if(socket != null) {

                if (inputStream != null && !socket.isClosed() && !socket.isInputShutdown()) {
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (outputStream != null && !socket.isClosed() && !socket.isOutputShutdown()) {
                    try {
                        outputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (!socket.isClosed()) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

}
