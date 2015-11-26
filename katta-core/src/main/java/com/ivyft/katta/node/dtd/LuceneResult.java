package com.ivyft.katta.node.dtd;

import com.ivyft.katta.node.io.UnbufferedDataInputInputStream;
import org.apache.hadoop.io.DataOutputOutputStream;
import org.apache.hadoop.io.Writable;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.JavaBinCodec;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-10-8
 * Time: 上午9:35
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public class LuceneResult implements Writable, Serializable {


    /**
     * 序列化表格
     */
    private final static long serialVersionUID = 1L;


    /**
     * 总共有多少个文档
     */
    private int total = 0;


    /** Stores the maximum score value encountered, needed for normalizing. */
    private float maxScore;

    /**
     * 当前遍历到文档的位移
     */
    private int start = 0;



    /**
     * 当前遍历到文档的位移
     */
    private int end = 0;


    /**
     * 一次性多个文档
     */
    private boolean mutil = false;


    /**
     * 一次性提取的文档数量
     */
    private short limit = 1000;


    /**
     * mutil=true时一次性提取多个文档
     */
    private List<SolrDocument> docs;

    /**
     * 构造方法
     */
    public LuceneResult() {

    }


    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public boolean isMutil() {
        return mutil;
    }

    public short getLimit() {
        return limit;
    }

    public void setLimit(short limit) {
        this.limit = limit;
    }


    public float getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(float maxScore) {
        this.maxScore = maxScore;
    }

    public List<SolrDocument> getDocs() {
        return docs;
    }

    public void setDocs(List<SolrDocument> docs) {
        this.docs = docs;
        this.mutil = (docs.size() > 1);
    }

    /**
     * Serialize the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(total);
        out.writeFloat(maxScore);
        out.writeInt(start);
        out.writeInt(end);
        out.writeBoolean(mutil);
        out.writeShort(limit);

        //检验是否有数据
        if(docs != null && !docs.isEmpty()) {
            //输出docs的长度
            out.writeInt(docs.size());

            JavaBinCodec codec = new JavaBinCodec();
            FastOutputStream daos = FastOutputStream.wrap(DataOutputOutputStream.constructOutputStream(out));
            codec.init(daos);
            try {
                for (SolrDocument doc : docs) {
                    codec.writeVal(doc);
                }
            } finally {
                daos.flushBuffer();
            }
        } else {
            out.writeInt(0);
        }
    }

    /**
     * Deserialize the fields of this object from <code>in</code>.
     * <p/>
     * <p>For efficiency, implementations should attempt to re-use storage in the
     * existing object where possible.</p>
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        total = in.readInt();
        maxScore = in.readFloat();
        start = in.readInt();
        end = in.readInt();
        mutil = in.readBoolean();
        limit = in.readShort();

        int length = in.readInt();
        if(length > 0) {
            if(docs == null) {
                docs = new LinkedList<SolrDocument>();
            }
            JavaBinCodec codec = new JavaBinCodec();
            UnbufferedDataInputInputStream dis = new UnbufferedDataInputInputStream(in);
            for(int i = 0; i < length; i++) {
                docs.add(((SolrDocument)codec.readVal(dis)));
            }
        } else {
            docs = new LinkedList<SolrDocument>();
        }
    }
}
