package com.ivyft.katta.ui.handle;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.exception.ExceptionUtils;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
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


    /**
     * Action Object
     */
    private final Object instance;


    /**
     * Action Method
     */
    private final Method method;


    /**
     * 输出的字符编码
     */
    protected Charset utf8 = Charset.forName("utf-8");


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

        ServletOutputStream out = resp.getOutputStream();

        try {
            Object r = method.invoke(instance, params, req, resp);
            if(r != null) {
                if(r instanceof String) {
                    resp.setCharacterEncoding(utf8.name());
                    resp.setContentType("text/html; charset=" + utf8);
                    resp.setHeader("Content-type", "text/html;charset=" + utf8);
                    new FreemarkerView((String)r).out(new OutputStreamWriter(out, utf8), params);
                } else if((r instanceof Map) || (r instanceof Collection)) {
                    resp.setCharacterEncoding("utf-8");
                    resp.setContentType("application/json");
                    resp.setHeader("Content-type", "application/json;charset=" + utf8);
                    out.write(JSON.toJSONString(r).getBytes(utf8));
                } else {
                    throw new IllegalArgumentException("unkown action to print " + r);
                }
            }
        } catch (Exception e) {
            resp.reset();
            resp.setCharacterEncoding(utf8.name());
            resp.setContentType("text/html; charset=" + utf8);
            resp.setHeader("Content-type", "text/html;charset=" + utf8);
            out.write(ExceptionUtils.getFullStackTrace(e).getBytes(utf8));
            throw new ServletException(e);
        } finally {
            out.flush();
        }
    }
}
