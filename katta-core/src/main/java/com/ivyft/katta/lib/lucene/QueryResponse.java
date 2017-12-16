package com.ivyft.katta.lib.lucene;

import com.google.common.collect.Ordering;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-12-19
 * Time: 下午1:07
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class QueryResponse implements Serializable {


    /**
     * 序列化
     */
    private final static long serialVersionUID = 1L;


    /**
     * 查询耗时
     */
    private long qTime = 0;


    /**
     * 查询到的总数量
     */
    private long numFount = 0;


    /**
     */
    private float maxScore = 0.0f;


    /**
     * 负载的数据
     */
    private final Collection<Map<String, Serializable>> docs;


    /**
     * 返回多少条记录
     */
    private int limit = 100;


    /**
     * 返回的起始位置
     */
    private int offset = 0;


    /**
     * 结果排序规则
     */
    private final SolrParams params;

    /**
     * 构造方法
     */
    public QueryResponse() {
        this(null);
    }


    public QueryResponse(SolrParams params) {
        this.params = params;
        if(params != null) {
            limit = params.getInt(CommonParams.ROWS, 100);
            offset = params.getInt(CommonParams.START, 0);

            docs = new LinkedList<>();
        } else {
            docs = new ArrayList<>(offset + limit);
        }
    }

    public long getQTime() {
        return qTime;
    }

    public void setQTime(long qTime) {
        this.qTime = Math.max(this.qTime, qTime);
    }

    public long getNumFount() {
        return numFount;
    }

    public void setNumFount(long numFount) {
        this.numFount = numFount;
    }


    public void addNumFount(long numFount) {
        this.numFount += numFount;
    }

    public float getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(float maxScore) {
        this.maxScore = Math.max(this.maxScore, maxScore);
    }

    public <T> Collection<T> getDocs() {
        return (Collection<T>) docs;
    }


    public synchronized void addDocs(Collection<Map<String, Serializable>> docs) {
        this.docs.addAll((Collection)docs);
        System.out.println("=====================");
        if(params != null) {

            String s = params.get(CommonParams.SORT);
            String[] fs = params.getParams("sort.fields.type");
            if(StringUtils.isNotBlank(s)) {

                Map<String, String> SORT_FIELD_MAP = new HashMap<>(fs.length);
                for (String f : fs) {
                    int beginIndex = f.indexOf(":");
                    SORT_FIELD_MAP.put(f.substring(0, beginIndex),
                            StringUtils.upperCase(f.substring(beginIndex+1)));
                }
                //需要排序
                System.out.println(SORT_FIELD_MAP);
                String[] sps = s.split(",");
                List<SolrQuery.SortClause> sorts = new ArrayList<>(sps.length);
                for (String sp : sps) {
                    String[] sc = sp.split("(\\s)+");
                    sorts.add(SolrQuery.SortClause.create(sc[0], sc[1]));
                }

                System.out.println(sorts);

                Comparator comparator;
                if(sorts.size() == 1) {
                    // 只有一个排序
                    String field = sorts.get(0).getItem();
                    String type = SORT_FIELD_MAP.get(field);
                    comparator = getComparator(type, field, "asc".equals(sorts.get(0).getOrder()));
                } else {
                    // 有 2 个以上的排序条件
                    List<Comparator> comparators = new ArrayList<>(sorts.size());
                    for (SolrQuery.SortClause sort : sorts) {
                        String type = SORT_FIELD_MAP.get(sort.getItem());

                        String field = sort.getItem();
                        comparators.add(getComparator(type, field, "asc".equals(sort.getOrder())));
                    }

                    comparator = new KattaListComparator(comparators);
                }

                Ordering<Map<String, Serializable>> orderList = Ordering.from(comparator);

                // 每次只要排序前的几百个，后面的几百个去掉
                Collection a = orderList.leastOf(docs, offset + limit);

                this.docs.clear();
                this.docs.addAll(a);
            }
        }
    }

    private Comparator getComparator(String type, String field, boolean asc) {
        switch (type) {
            case "STRING":
                // 字符串类型排序
                return new StringComparator(field, asc);

            case "INTEGER":
                // 整形类型排序
                return new IntegerComparator(field, asc);

            case "BOOLEAN":
                // Boolean 类型排序
                return new BooleanComparator(field, asc);

            default:
                System.out.println("unknown type");
                //throw new IllegalArgumentException("unknown sort type: " + type + " from " + field);
        }
        return new TextComparator(field, asc);
    }

}


class KattaListComparator implements Comparator<Map<String, Serializable>> {

    protected List<Comparator> comparators;


    public int asc = 1;



    public KattaListComparator(List<Comparator> comparators) {
        this(comparators, false);
    }



    public KattaListComparator(List<Comparator> comparators, boolean desc) {
        this.comparators = comparators;
        this.asc = (desc ? -1 : 1);
    }

    @Override
    public int compare(Map<String, Serializable> o1, Map<String, Serializable> o2) {
        for (Comparator comparator : comparators) {
            int compare = comparator.compare(o1, o2);
            if(compare != 0) {
                return this.asc * compare;
            }
        }
        return 0;
    }
}

class StringComparator implements Comparator<Map<String, Serializable>> {

    public int asc = 1;


    protected String field;


    public StringComparator(String field) {
        this(field, false);
    }

    public StringComparator(String field, boolean desc) {
        this.field = field;
        this.asc = (desc ? -1 : 1);
    }

    @Override
    public int compare(Map<String, Serializable> o1, Map<String, Serializable> o2) {
        return asc * ((String)o1.get(field)).compareTo((String)o2.get(field));
    }
}


class TextComparator extends StringComparator {

    public TextComparator(String field) {
        super(field);
    }

    public TextComparator(String field, boolean desc) {
        super(field, desc);
    }


    @Override
    public int compare(Map<String, Serializable> o1, Map<String, Serializable> o2) {
        return asc * (String.valueOf(o1.get(field)).compareTo(String.valueOf(o2.get(field))));
    }
}


class IntegerComparator implements Comparator<SolrDocument> {

    public int asc = 1;


    protected String field;

    public IntegerComparator(String field) {
        this(field, true);
    }

    public IntegerComparator(String field, boolean desc) {
        this.field = field;
        this.asc = (desc ? -1 : 1);
    }

    @Override
    public int compare(SolrDocument o1, SolrDocument o2) {
        return asc * ((Integer)o1.get(field)).compareTo((Integer) o2.get(field));
    }
}


class DoubleComparator implements Comparator<SolrDocument> {

    public int asc = 1;


    protected String field;

    public DoubleComparator(String field) {
        this(field, true);
    }

    public DoubleComparator(String field, boolean desc) {
        this.field = field;
        this.asc = (desc ? -1 : 1);
    }

    @Override
    public int compare(SolrDocument o1, SolrDocument o2) {
        return asc * ((Double)o1.get(field)).compareTo((Double) o2.get(field));
    }
}



class BooleanComparator implements Comparator<SolrDocument> {

    public int asc = 1;


    protected String field;

    public BooleanComparator(String field) {
        this(field, true);
    }

    public BooleanComparator(String field, boolean desc) {
        this.field = field;
        this.asc = (desc ? -1 : 1);
    }

    @Override
    public int compare(SolrDocument o1, SolrDocument o2) {
        return asc * ((Boolean)o1.get(field)).compareTo((Boolean) o2.get(field));
    }
}