package com.ivyft.katta.ui.controller;

import com.google.common.collect.ImmutableMap;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.protocol.metadata.MasterMetaData;
import com.ivyft.katta.protocol.metadata.NodeMetaData;
import com.ivyft.katta.ui.annaotion.Action;
import com.ivyft.katta.ui.annaotion.Path;
import com.ivyft.katta.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;
import org.joda.time.DateTime;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class DashboardController {


    protected ZkClient zklcient;


    protected InteractionProtocol protocol;


    public DashboardController() {
        ZkConfiguration conf = new ZkConfiguration();
        zklcient = new ZkClient(conf.getZKServers(), conf.getZKTickTime(), conf.getZKTimeOut());
        protocol = new InteractionProtocol(zklcient, conf);
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
        List<NodeMetaData> nodes = new ArrayList<>();

        for (String liveNode : liveNodes) {
            nodes.add(protocol.getNodeMD(liveNode));
        }

        List<String> knownNodes = protocol.getKnownNodes();
        for (String knownNode : knownNodes) {
            NodeMetaData nodeMetaData = new NodeMetaData(knownNode);
            nodeMetaData.setStartTimeStamp(dateTime.getMillis());
            nodeMetaData.setQueriesPerMinute(0);

            nodes.add(nodeMetaData);
        }

        params.put("nodes", nodes);

        List<String> indices = protocol.getIndices();
        List<IndexMetaData> indexMetaDataList = new ArrayList<>(indices.size());
        for (String index : indices) {
            indexMetaDataList.add(protocol.getIndexMD(index));
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

        return "indice.ftl";
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
