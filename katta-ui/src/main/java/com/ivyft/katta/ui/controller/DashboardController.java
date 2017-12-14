package com.ivyft.katta.ui.controller;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.ivyft.katta.protocol.metadata.IndexDeployError;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.protocol.metadata.NodeMetaData;
import com.ivyft.katta.ui.annaotion.Action;
import com.ivyft.katta.ui.annaotion.Path;

import org.I0Itec.zkclient.ZkClient;
import org.joda.time.DateTime;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
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


    ZkClient zklcient;



    public DashboardController() {
        zklcient = new ZkClient("localhost:2181", 60000, 30000);
    }


    @Path("/dashboard")
    public String dashboard(Map<String, Object> params, HttpServletRequest request, HttpServletResponse response) {
        HashMap<String, Object> map = new HashMap<>();
        map.put("id", "asdfasdfad");
        map.put("name", "asdfasdfad");
        map.put("nodeCount", 10);
        map.put("indexCount", 10);
        map.put("uri", "asdfasd");

        params.put("title", "Katta Dashboard");
        params.put("cluster", map);

        params.put("flash", ImmutableMap.of("message", "OK"));

        params.put("master", ImmutableMap.of("masterName", "3nqrjhewhr234234123h", "startTimeAsString", new Date()));

        NodeMetaData nodeMetaData = new NodeMetaData("zhenqin-pro102");
        nodeMetaData.setStartTimeStamp(new DateTime(2017, 12, 12, 19, 0, 0).getMillis());
        nodeMetaData.setQueriesPerMinute(10);

        params.put("nodes", Lists.newArrayList(nodeMetaData));

        IndexMetaData indexMetaData = new IndexMetaData("aaa", "hdfs:/user/hadoop/fidl", "userindex", 3);
        params.put("indexes", ImmutableList.of(indexMetaData));

        return "dashboard.ftl";
    }
}
