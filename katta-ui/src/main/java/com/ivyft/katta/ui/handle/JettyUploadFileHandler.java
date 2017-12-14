package com.ivyft.katta.ui.handle;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableSet;
import freemarker.template.TemplateException;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.*;

/**
 * Created by ninggd on 2017/7/26.
 */
public class JettyUploadFileHandler extends HttpServlet {

    protected static Logger logger = LoggerFactory.getLogger("JettyUploadFileHandler");

    // 上传文件存储目录
    private static final String UPLOAD_DIRECTORY = "upload";



    /**
     * 上传路径（HDFS路径）
     */
    protected final String uploadPath;

    // 上传配置
    private static final int MAX_FILE_SIZE      = 1024 * 1024 * 50; // 50MB
    private static final int MAX_REQUEST_SIZE   = 1024 * 1024 * 100; // 100MB

    /**
     * 文本文件类型
     */
    public final static Set<String> TXT_FILE_SUBFIX = ImmutableSet.of("txt", "json");


    /**
     * 压缩包文件类型
     */
    public final static Set<String> COMP_FILE_SUBFIX = ImmutableSet.of("zip", "tar", "tgz", "gz");



    public JettyUploadFileHandler(String path) {
        this.uploadPath = path;

        try {
            File upload = new File(uploadPath);
            if(!upload.exists()) {
                if(!upload.mkdirs()) {
                    throw new IOException("can't create dir: " + uploadPath);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("can't create dir: " + uploadPath, e);
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("text/html");
        resp.setCharacterEncoding("utf-8");

        PrintWriter writer = resp.getWriter();

        try {
            new FreemarkerServlet("upload_file.ftl").out(writer, new HashMap<>());
        } catch (TemplateException e) {
            throw new ServletException(e);
        }
        writer.flush();
    }



    protected void doGetJson(HttpServletRequest req,
                             HttpServletResponse resp,
                             JSONObject responseJSON) throws ServletException, IOException {
        resp.setContentType("application/json");
        resp.setCharacterEncoding("utf-8");

        PrintWriter writer = resp.getWriter();

        writer.println(responseJSON.toJSONString());
        writer.flush();
        writer.close();
    }


    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // 创建文件上传核心类
        DiskFileItemFactory factory = new DiskFileItemFactory();
        ServletFileUpload sfu = new ServletFileUpload(factory);
        //设置编码
        sfu.setHeaderEncoding("UTF-8");
        // 设置上传的单个文件的最大字节数为2M
        sfu.setFileSizeMax(MAX_FILE_SIZE);
        //设置整个表单的最大字节数为10M
        sfu.setSizeMax(MAX_REQUEST_SIZE);
        List<FileItem> formItems = null;

        JSONObject responseJSON = new JSONObject();

        try {
            // 解析请求的内容提取文件数据
            formItems = sfu.parseRequest(req);
            if(formItems == null || formItems.isEmpty()) {
                doGet(req, resp);
                return;
            }
        } catch (FileUploadException e) {
            logger.info("上文文件失败。", e);
            responseJSON.put("success", false);
            responseJSON.put("msg", ExceptionUtils.getFullStackTrace(e));

            doGetJson(req, resp, responseJSON);
            return;
        }

        // 普通文本表单
        int formCount = 0;
        Map<String, String> formParams = new HashMap(formItems.size());
        for (FileItem fileItem : formItems) {
            formCount++;
            // 对应表单中的控件的name
            String fieldName = fileItem.getFieldName();
            // 如果是普通表单控件
            if (fileItem.isFormField()) {
                String value = fileItem.getString();
                //重新编码,解决乱码
                value = new String(value.getBytes("ISO-8859-1"), "UTF-8");
                formParams.put(fieldName, value);
            }
        }

        logger.info("表单参数：{}", formParams.toString());

        // 构造临时路径来存储上传的文件
        // 这个路径相对当前应用的目录

        String dataType = formParams.get("dataType");
        String projectId = formParams.get("projectId");

        if(formParams.isEmpty() || StringUtils.isBlank(dataType) || StringUtils.isBlank(projectId)) {
            logger.warn("存储文件失败，缺少 dataType 和 projectId 参数。");
            responseJSON.put("success", false);
            responseJSON.put("msg", "缺少 dataType 和 projectId 参数。");

            doGetJson(req, resp, responseJSON);
            return;
        }

        if(formParams.size() == formCount) {
            logger.warn("没有数据文件。");
            responseJSON.put("success", false);
            responseJSON.put("msg", "没有数据文件。");

            doGetJson(req, resp, responseJSON);
            return;
        }

        StringBuffer childPath = new StringBuffer();
        String timeStr = DateTime.now().toString("yyyyMMdd");
        childPath.append("/").append(timeStr);

        //上传hdfs文件路径
        String filePath = uploadPath + childPath;
//        Path outPath = new Path(filePath);
        logger.info("抓取数据上传至 : " + filePath);

        final List<String> files = new ArrayList<String>();
        for (FileItem item : formItems) {
            // 处理不在表单中的字段
            if (!item.isFormField()) {
                // 获得文件大小
                OutputStream out = null;
                InputStream in = null;
                // 获得文件名
                String fileName = item.getName();
                File outPath = new File(filePath, fileName);

                try {
                    out = new FileOutputStream(outPath, false);
                    in = item.getInputStream();
                    long size = item.getSize();   //item 表示的是文件
                    logger.info("文件名：" + fileName + "    大小：" + size + "byte");
                    IOUtils.copy(in, out);       //上传文件到hdfs.
                    out.flush();
                    logger.info("upload file [" + fileName + "] is done");
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.warn("存储文件失败。", e);
                    responseJSON.put("success", false);
                    responseJSON.put("msg", ExceptionUtils.getFullStackTrace(e));

                    doGetJson(req, resp, responseJSON);
                    return;
                } finally {
                    IOUtils.closeQuietly(out);
                    IOUtils.closeQuietly(in);
                }

                // 如果是压缩包，解压到uploadPath，
                logger.info("上传文件路径为：" + outPath);

                responseJSON.put("success", true);
                responseJSON.put("msg", "upload file is successful");
            }
        }

        if(files.isEmpty()) {
            logger.warn("没有解压到数据文件。");
            responseJSON.put("success", false);
            responseJSON.put("msg", "没有解压到数据文件。");

            doGetJson(req, resp, responseJSON);
            return;
        }

        //上传完毕后  转发到首页
        doGetJson(req, resp, responseJSON);

    }

    public static String getFileSubfix(String name) {
        int i = name.lastIndexOf(".");
        if(i > 0) {
            return StringUtils.trimToNull(name.substring(i+1));
        }
        return null;
    }
}
