package com.ivyft.katta.hadoop;

import com.google.common.collect.Sets;
import com.ivyft.katta.node.dtd.LuceneQuery;
import com.ivyft.katta.node.dtd.LuceneResult;
import com.ivyft.katta.node.dtd.Next;
import com.ivyft.katta.node.dtd.OK;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CommonParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Iterator;

/**
 * <pre>
 * <p>
 * </p>
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-9-29
 * Time: 下午3:39
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public class KattaSocketReader {

    /**
     * split
     */
	private KattaInputSplit _split;


	/**
	 * mutil=true时一次性提取多个文档
	 */
	private Iterator<SolrDocument> _cursor;


    /**
     * current
     */
	private SolrDocument _current;


    /**
     * Lucene
     */
	private LuceneResult luceneResult;


    /**
     * next
     */
	private boolean next = false;


    /**
     * 打开的Socket
     */
    private Socket socket = null;


    /**
     * 输入
     */
    private DataInputStream inputStream = null;


    /**
     * 输出
     */
    private ObjectOutputStream outputStream = null;

	/**
	 * 日志记录
	 */
	private static Logger log = LoggerFactory.getLogger(KattaSocketReader.class);

	/**
	 * 构造方法
	 *
	 * @param split
	 */
	public KattaSocketReader(KattaInputSplit split) {
		this._split = split;
	}

	public void initialize(InputSplit split)
			throws IOException {
        // 创建无连接传输channel的辅助类(UDP),包括client和server
        socket = new Socket(this._split.getHost(),
                this._split.getPort());

		/*
		 */
		while (!socket.isConnected()) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
		}

        try {
            outputStream = new ObjectOutputStream(socket.getOutputStream());
            inputStream = new DataInputStream(socket.getInputStream());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

		log.info("create " + this._split.getHost() + ":" + this._split.getPort()+" socket success...");

        //先查询solr，结果会被放在solr插件netty的临时变量里面
        SolrQuery solrQuery = _split.getQuery();
        LuceneQuery query = new LuceneQuery(_split.getShardName(), solrQuery);
        String fields = solrQuery.get(CommonParams.FL);
        if(StringUtils.isNotBlank(fields)) {
            query.setFields(Sets.newHashSet(fields.split(",")));
            log.info("SolrIncludeFields:" + fields);
        }

        outputStream.writeObject(query);

        try {
            outputStream.flush();
        } catch (Exception e) {
            log.warn("", e);
        }
		// 获得第一批数据
        Next next = new Next(_split.getStart(), (short) _split.getLimit(), _split.getMaxDocs());

        try {
            outputStream.writeObject(next);

            outputStream.flush();

            //luceneResult = (LuceneResult)inputStream.readObject();
            luceneResult = new LuceneResult();
            luceneResult.readFields(inputStream);
            _cursor = luceneResult.getDocs().iterator();


            try {
                //Object OutputStream 在输出时会记录每个输出对象, 不会释放内存.
                //这里 reset 为了防止内存泄露
                outputStream.reset();
            } catch (Exception e) {
                log.warn("", e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
	}



	public synchronized boolean nextKeyValue() throws IOException {
        // 当前_cursor已经没数据了
        if (!_cursor.hasNext()) {
            int total = Math.min(_split.getMaxDocs(), luceneResult.getTotal());
            if(luceneResult.getEnd() >= total) {
                // 接收完成，发送一个 OK 对象，服务器端会退出
                outputStream.writeObject(new OK());
                return false;
            }
            // end 小于 total，则说明远程还有数据，需要继续读取
            next = true;
            // 进入下面的if说明远程也没了
            if (next) {
                // 远程有呢，就送远程继续拿，放到当前的_cursor
                // 获得第一批数据
                try {
                    luceneResult = new LuceneResult();
                    luceneResult.readFields(inputStream);
                    _cursor = luceneResult.getDocs().iterator();
                    _current = _cursor.next();
                } catch (EOFException e) {
                    // 远程接口已经关闭，读取到末尾了
                    // 接收完成，发送一个 OK 对象，服务器端会退出
                    outputStream.writeObject(new OK());
                    return false;
                } catch (Exception e) {
                    // 接收完成，发送一个 OK 对象，服务器端会退出
                    outputStream.writeObject(new OK());
                    log.error(ExceptionUtils.getFullStackTrace(e));
                    return false;
                }
            }
            return next;
        }

		next = true;
		_current = _cursor.next();
		return next;

	}

	public Object getCurrentKey() throws IOException {
		return _current.get(_split.getKeyField());
	}

	public SolrDocument getCurrentValue() throws IOException {
		return _current;
	}

	public float getProgress() throws IOException {
        float process = 0.0f;
        if(luceneResult != null) {
            //防止除以0抛异常
            if( luceneResult.getTotal() > 0) {
                process = luceneResult.getEnd() * 1.0f / luceneResult.getTotal();
            }
        }
		return process;
	}


	public long getPos() {
        long pos = 0L;
        if(luceneResult != null) {
            //防止除以0抛异常
            pos = luceneResult.getEnd();
        }
        return pos;
    }

    /**
     * Close the record reader.
     */
    public void close() throws IOException {
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException e) {
                log.warn("", e);
            }
        }
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                log.warn("", e);
            }
        }
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                log.warn("", e);
            }
        }
    }
}
