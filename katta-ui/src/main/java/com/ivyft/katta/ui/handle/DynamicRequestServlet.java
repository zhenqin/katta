package com.ivyft.katta.ui.handle;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.exception.ExceptionUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 17/12/13
 * Time: 17:51
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class DynamicRequestServlet extends HttpServlet {

    Object instance;
    Method method;


    public DynamicRequestServlet(Object instance, Method method) {
        this.instance = instance;
        this.method = method;
    }


    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        HashMap<String, Object> params = new HashMap<>();
        params.put("request", req);

        PrintWriter writer = resp.getWriter();

        try {
            Object r = method.invoke(instance, params, req, resp);
            if(r != null) {
                if(r instanceof String) {
                    resp.setContentType("text/html");
                    resp.setCharacterEncoding("utf-8");
                    new FreemarkerServlet((String)r).out(writer, params);
                } else if((r instanceof Map) || (r instanceof Collection)) {
                    resp.setContentType("application/json");
                    resp.setCharacterEncoding("utf-8");
                    writer.write(JSON.toJSONString(r));
                } else {
                    throw new IllegalArgumentException("unkown action to print " + r);
                }
            }
        } catch (Exception e) {
            resp.reset();
            writer.write(ExceptionUtils.getFullStackTrace(e));
            throw new ServletException(e);
        } finally {
            writer.flush();
        }
    }
}
