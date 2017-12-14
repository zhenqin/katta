package com.ivyft.katta.ui.handle;

import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 17/12/13
 * Time: 15:15
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class FreemarkerServlet extends HttpServlet {



    final Template template;

    final Map<String, Object> params;

    public FreemarkerServlet(String view) {
        this(view, new HashMap<>());
    }

    public FreemarkerServlet(String view, Map<String, Object> params) {
        if(params == null) {
            params = new HashMap<>(0);
        }
        this.params = params;
        try {
            this.template = FreemarkerView.conf.getTemplate(view, "UTF-8");
        } catch (IOException e) {
            throw new IllegalStateException("unload template. ", e);
        }
    }



    public String getViewString(Map<String, Object> params) throws IOException, TemplateException {
        StringWriter stringWriter = new StringWriter();
        template.process(params, stringWriter);
        return stringWriter.toString();
    }




    public void out(PrintWriter writer, Map<String, Object> params) throws IOException, TemplateException {
        template.process(params, writer);
    }


    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        this.doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("text/html");
        resp.setCharacterEncoding("utf-8");

        PrintWriter writer = resp.getWriter();

        try {
            HashMap<String, Object> map = new HashMap<>(params);
            map.put("request", req);

            template.process(map, writer);
        } catch (TemplateException e) {
            throw new ServletException(e);
        }
        writer.flush();
    }



    public static void main(String[] args) throws IOException, TemplateException {
        FreemarkerServlet servlet = new FreemarkerServlet("upload_file.ftl");
        System.out.println(servlet.getViewString(new HashMap<>()));

        servlet = new FreemarkerServlet("upload_file.ftl");
        System.out.println(servlet.getViewString(new HashMap<>()));
    }


}
