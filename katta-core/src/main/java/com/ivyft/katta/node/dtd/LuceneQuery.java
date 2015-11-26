package com.ivyft.katta.node.dtd;

import org.apache.solr.client.solrj.SolrQuery;

import java.io.Serializable;
import java.util.Set;

/**
 * <pre>
 * 
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-10-8
 * Time: 上午9:14
 * To change this template use File | Settings | File Templates.
 * 
 * </pre>
 * 
 * @author ZhenQin
 */
public class LuceneQuery implements Serializable {

	/**
	 * 序列化表格
	 */
	private final static long serialVersionUID = 1L;


    /**
     * 需要查询的ShardName
     */
    private String shardName;


	/**
	 * 结果集中是否增加评分
	 */
	private boolean addScore = false;

	/**
	 * 默认搜索的Field, 如果为null则添加所有field到结果集中
	 */
	private Set<String> fields = null;


    /**
     * 默认搜索的Field
     */
    private String searchField;


    /**
     * Lucene查询字符串, 不同于Solr
     */
    private SolrQuery solrQuery;

	/**
	 * Lucene ArrayQuery
	 */
	private int max = Integer.MAX_VALUE;

    /**
     * 构造方法
     *
     * @param solrQuery
     *            查询Lucene字符串,类似于Solr
     */
    public LuceneQuery(String shardName, SolrQuery solrQuery) {
        this.shardName = shardName;
        this.solrQuery = solrQuery;
    }


    public String getShardName() {
        return shardName;
    }


    public boolean isAddScore() {
		return addScore;
	}

	public void setAddScore(boolean addScore) {
		this.addScore = addScore;
	}

	public Set<String> getFields() {
		return fields;
	}

	public void setFields(Set<String> fields) {
        if(fields.isEmpty()) {
            throw new IllegalArgumentException("fields length > 0.");
        }
		this.fields = fields;
	}

    public SolrQuery getSolrQuery() {
        return solrQuery;
    }

    public String getSearchField() {
        return searchField;
    }

    public void setSearchField(String searchField) {
        this.searchField = searchField;
    }

    public int getMax() {
		return max;
	}

	public void setMax(int max) {
		this.max = max;
	}

	@Override
	public String toString() {
		return "guery:" +getSolrQuery() + "  fileds:" + this.getFields()
				+ "  addScore :" + this.isAddScore();
	}
}
