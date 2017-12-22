package com.ivyft.katta.ui.controller;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Singleton;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.protocol.metadata.MasterMetaData;
import com.ivyft.katta.protocol.metadata.NodeMetaData;
import com.ivyft.katta.ui.annaotion.Action;
import com.ivyft.katta.ui.annaotion.Path;
import com.ivyft.katta.ui.dtd.IndexInfo;
import com.ivyft.katta.ui.dtd.NodeInfo;
import com.ivyft.katta.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;
import org.joda.time.DateTime;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.*;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 17/12/13
 * Time: 17:18
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
@Action
@Singleton
public class DashboardController {


    protected ZkClient zklcient;


    protected InteractionProtocol protocol;


    @Inject
    public DashboardController(ZkClient zklcient, InteractionProtocol protocol) {
        this.zklcient = zklcient;
        this.protocol = protocol;
    }


    @Path("/overview")
    public String overview(Map<String, Object> params,
                            HttpServletRequest request,
                            HttpServletResponse response) {
        return dashboard(params, request, response);
    }

    @Path("/dashboard")
    public String dashboard(Map<String, Object> params,
                            HttpServletRequest request,
                            HttpServletResponse response) {
        HashMap<String, Object> map = new HashMap<>();
        map.put("id", "asdfasdfad");
        map.put("name", "asdfasdfad");
        map.put("nodeCount", 10);
        map.put("indexCount", 10);
        map.put("uri", "asdfasd");

        params.put("title", "Katta Dashboard");
        params.put("cluster", map);

        params.put("flash", ImmutableMap.of("message", "OK"));

        DateTime dateTime = new DateTime(2017, 12, 12, 19, 0, 0);

        MasterMetaData masterMetaData = new MasterMetaData();
        masterMetaData.setMasterName("aaaabbbb-cccdd-asdfa");
        masterMetaData.setStartTime(dateTime.getMillis());
        masterMetaData.setProxyBlckPort(4560);

        MasterMetaData masterMD = protocol.getMasterMD();

        params.put("master", masterMD);



        List<String> liveNodes = protocol.getLiveNodes();
        List<NodeInfo> nodes = new ArrayList<>();

        for (String liveNode : liveNodes) {
            Collection<String> nodeShards = protocol.getNodeShards(liveNode);
            NodeMetaData nodeMD = protocol.getNodeMD(liveNode);

            NodeInfo nodeInfo = new NodeInfo(nodeMD);
            nodeInfo.setShards(nodeShards);
            nodeInfo.setLive("live");
            nodes.add(nodeInfo);
        }

        List<String> knownNodes = protocol.getKnownNodes();
        for (String knownNode : knownNodes) {
            NodeMetaData nodeMetaData = new NodeMetaData(knownNode);
            nodeMetaData.setStartTimeStamp(dateTime.getMillis());
            nodeMetaData.setQueriesPerMinute(0);

            boolean add = true;
            for (NodeInfo node : nodes) {
                if(node.getName().equals(knownNode)) {
                    add = false;
                    break;
                }
            }

            if(add) {
                NodeInfo nodeInfo = new NodeInfo(nodeMetaData);
                nodeInfo.setLive("down");
                nodes.add(nodeInfo);
            }
        }

        params.put("nodes", nodes);

        List<String> indices = protocol.getIndices();
        List<IndexInfo> indexMetaDataList = new ArrayList<>(indices.size());
        for (String index : indices) {
            IndexMetaData indexMD = protocol.getIndexMD(index);
            indexMetaDataList.add(new IndexInfo(indexMD));
        }
        params.put("indexes", indexMetaDataList);

        return "dashboard.ftl";
    }



    /**
     * RESTFull
     * @param params
     * @return
     */
    @Path("/indexx")
    public String indexName(Map<String, Object> params,
                                         HttpServletRequest request,
                                         HttpServletResponse response) {
        String indexName = request.getParameter("name");

        params.put("name", indexName);
        params.put("success", true);
        params.put("datetime", System.currentTimeMillis());
        params.put("message", "我是傻子？ hi "+indexName+".");

        IndexMetaData indexMD = protocol.getIndexMD(indexName);
        params.put("index", new IndexInfo(indexMD));
        return "indice.ftl";
    }


    public List<String> getLiveNodes() {
        return protocol.getLiveNodes();
    }

    /**
     * RESTFull
     * @param params
     * @return
     */
    @Path("/node")
    public String node(Map<String, Object> params,
                            HttpServletRequest request,
                            HttpServletResponse response) {
        String nodeName = request.getParameter("name");

        params.put("name", nodeName);
        params.put("success", true);
        params.put("datetime", System.currentTimeMillis());
        params.put("message", "我是傻子？ hi "+nodeName+".");

        Collection<String> nodeShards = protocol.getNodeShards(nodeName);
        params.put("shards", nodeShards);

        Map<String, List<String>> shard2NodesMap = protocol.getShard2NodesMap(nodeShards);

        params.put("shard2Node", shard2NodesMap);
        return "node.ftl";
    }

    /**
     * RESTFull
     * @param params
     * @return
     */
    @Path("/hi")
    public Map<String, Object> hi(Map<String, Object> params, HttpServletRequest request, HttpServletResponse response) {
        String name = request.getParameter("name");

        Map<String, Object> r = new HashMap<>();
        r.put("name", name);
        r.put("error", "我这里是中文。");
        r.put("success", true);
        r.put("datetime", System.currentTimeMillis());
        r.put("message", "hi "+name+".");

        return r;
    }


}
