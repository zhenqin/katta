package com.ivyft.katta.ui.handle;

import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
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
public class FreemarkerView {

    static Configuration conf = new Configuration(Configuration.VERSION_2_3_23);

    static {

        try {
            //conf.setTemplateLoader(new FileTemplateLoader(new File("src/main/resources/templates"), true));
            conf.setTemplateLoader(new ClassTemplateLoader(Thread.currentThread().getContextClassLoader(), "/templates"));
            conf.setNumberFormat("#");//防止页面输出数字,变成2,000
            //可以添加很多自己的要传输到页面的[方法、对象、值]
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    final String view;



    final Template template;

    public FreemarkerView(String view) {
        this.view = view;
        try {
            this.template = conf.getTemplate(view, "UTF-8");
        } catch (IOException e) {
            throw new IllegalStateException("unload template. ", e);
        }
    }



    public String getViewString(Map<String, Object> params) throws IOException, TemplateException {
        StringWriter stringWriter = new StringWriter();
        template.process(params, stringWriter);
        return stringWriter.toString();
    }




    public void out(Writer writer, Map<String, Object> params) throws IOException, TemplateException {
        template.process(params, writer);
    }



    public static void main(String[] args) throws IOException, TemplateException {
        FreemarkerView servlet = new FreemarkerView("upload_file.ftl");
        System.out.println(servlet.getViewString(new HashMap<>()));

        System.out.println(servlet.template.getEncoding());
    }


}
