package com.ivyft.katta.client;

import com.ivyft.katta.lib.lucene.*;
import com.ivyft.katta.server.protocol.KattaServerProtocol;
import com.ivyft.katta.util.KattaException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.solr.client.solrj.SolrQuery;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/2/26
 * Time: 21:41
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class SingleKattaClient implements ISolrClient, ISingleKattaClient {

    /**
     * Katta Server Proxy
     */
    private final ProtocolProxy<KattaServerProtocol> protocolProxy;


    /**
     * Katta Server 远程查询接口
     */
    protected final KattaServerProtocol kattaServerProtocol;


    /**
     * 超时时间
     */
    protected long timeout = 10 * 1000L;


    /**
     * 构造
     *
     * @param conf Hadoop Conf
     * @param host Katta Server host
     * @param port Katta Server port
     *
     * @throws IOException
     */
    public SingleKattaClient(Configuration conf, String host, int port) throws IOException {
        protocolProxy = RPC.getProtocolProxy(KattaServerProtocol.class,
                ILuceneServer.versionID, new InetSocketAddress(host, port), conf);
        this.kattaServerProtocol = protocolProxy.getProxy();
    }

    @Override
    public QueryResponse query(SolrQuery query, String[] shards) throws KattaException {
        try {
            ResponseWritable query1 = kattaServerProtocol.query(new QueryWritable(query), shards, timeout);
            return query1.getResponse();
        } catch (IOException e) {
            throw new KattaException(e);
        }
    }

    @Override
    public <E> Set<E> group(SolrQuery query, String[] shards) throws KattaException {
        try {
            GroupResultWritable group = kattaServerProtocol.group(new QueryWritable(query), shards, timeout);
            return (Set)group.get();
        } catch (IOException e) {
            throw new KattaException(e);
        }
    }

    @Override
    public <E> Map<E, Integer> facet(SolrQuery query, String[] shards) throws KattaException {
        try {
            FacetResultWritable facet = kattaServerProtocol.facet(new QueryWritable(query), shards, timeout);
            return facet.get();
        } catch (IOException e) {
            throw new KattaException(e);
        }
    }

    @Override
    public <E> Map<E, Integer> facetRange(SolrQuery query, String[] shards) throws KattaException {
        try {
            FacetResultWritable facetByRange = kattaServerProtocol.facetByRange(new QueryWritable(query), shards, timeout);
            return facetByRange.get();
        } catch (IOException e) {
            throw new KattaException(e);
        }
    }

    @Override
    public Hits search(SolrQuery query, String[] shards) throws KattaException {
        try {
            HitsMapWritable search = kattaServerProtocol.search(new QueryWritable(query), shards, timeout);
            Hits hits = new Hits();
            hits.addHits(search.getHitList());
            hits.setTotalHits(search.getTotalHits());
            return hits;
        } catch (IOException e) {
            throw new KattaException(e);
        }
    }

    @Override
    public MapWritable getDetail(Hit hit) throws KattaException {
        try {
            return kattaServerProtocol.getDetail(new String[]{hit.getShard()}, hit.getDocId());
        } catch (IOException e) {
            throw new KattaException(e);
        }
    }

    @Override
    public MapWritable getDetail(Hit hit, String[] fields) throws KattaException {
        try {
            return kattaServerProtocol.getDetail(new String[]{hit.getShard()}, hit.getDocId(), fields);
        } catch (IOException e) {
            throw new KattaException(e);
        }
    }

    @Override
    public List<MapWritable> getDetails(List<Hit> hits) throws KattaException, InterruptedException {
        try {
            List<MapWritable> r = new ArrayList<MapWritable>(hits.size());
            for (Hit hit : hits) {
                r.add(kattaServerProtocol.getDetail(new String[]{hit.getShard()}, hit.getDocId()));
            }
            return r;
        } catch (IOException e) {
            throw new KattaException(e);
        }
    }

    @Override
    public List<MapWritable> getDetails(List<Hit> hits, String[] fields) throws KattaException, InterruptedException {
        try {
            List<MapWritable> r = new ArrayList<MapWritable>(hits.size());
            for (Hit hit : hits) {
                r.add(kattaServerProtocol.getDetail(new String[]{hit.getShard()}, hit.getDocId(), fields));
            }
            return r;
        } catch (IOException e) {
            throw new KattaException(e);
        }
    }

    @Override
    public double getQueryPerMinute() {
        return 0;
    }

    @Override
    public int count(SolrQuery query, String[] shards) throws KattaException {
        try {
            return kattaServerProtocol.count(new QueryWritable(query), shards, timeout);
        } catch (IOException e) {
            throw new KattaException(e);
        }
    }


    @Override
    public void close() {
        RPC.stopProxy(this.protocolProxy);
    }

    @Override
    public void addIndex(String name, String solr, URI path) throws KattaException {
        try {
            kattaServerProtocol.addShard(name, solr, path.toString());
        } catch (IOException e) {
            throw new KattaException(e);
        }
    }

    @Override
    public void removeIndex(String name) throws KattaException {
        try {
            kattaServerProtocol.removeShard(name);
        } catch (IOException e) {
            throw new KattaException(e);
        }
    }



    public KattaServerProtocol getKattaServerProtocol() {
        return kattaServerProtocol;
    }


    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

}
