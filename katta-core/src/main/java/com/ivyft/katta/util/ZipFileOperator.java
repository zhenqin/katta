package com.ivyft.katta.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

public class ZipFileOperator {

    /**
     * 日志记录
     *
     */
    private Log log = LogFactory.getLog(getClass());


    /**
     * 压缩指定文件集.
     * @param compressFilePath 压缩后的zip文件保存的路径
     * @param filesPath 需要被压缩的文件或者文件夹集合
     * @return 返回压缩后的文件
     * @throws IOException  压缩异常
     */
    public File compressZip(String compressFilePath, String... filesPath) throws IOException {
        return compressZip(new File(compressFilePath), filesPath);
    }


    /**
     * 压缩指定文件集.
     *
     * @param files
     *            被压缩的文件集合
     * @param compressFile
     *            压缩后的zip文件保存的路径
     * @return 返回压缩后的文件
     * @throws IOException
     *             压缩异常
     */
    public File compressZip(File compressFile, String... files)
            throws IOException {
        if (!compressFile.exists()) {
            if (!compressFile.getParentFile().exists()) {
                if (!compressFile.getParentFile().mkdirs()) {
                    StringBuilder exception = new StringBuilder();
                    exception.append("系统找不到指定的路径: ");
                    exception.append(compressFile.getParentFile().getAbsolutePath());
                    exception.append("并且创建: ");
                    exception.append(compressFile.getAbsolutePath());
                    exception.append("失败!");
                    throw new IOException(exception.toString());
                }
            }
            if (!compressFile.createNewFile()) {
                StringBuilder exception = new StringBuilder();
                exception.append("创建文件: ");
                exception.append(compressFile.getAbsolutePath());
                exception.append("失败!");
                throw new IOException(exception.toString());
            }
        }
        ZipOutputStream zos = null;
        try {
            zos = new ZipOutputStream(
                    new FileOutputStream(compressFile));
            for (String fileName : files) {
                File compFile = new File(fileName);
                if(compFile.exists()){
                    compressZip0(zos, compFile, compFile.getName());
                }
            }
            // 当压缩完成,关闭流
            zos.closeEntry();
        } catch(Exception e) {
            log.warn(e);
        } finally {
            if(zos != null){
                zos.close();
            }
        }
        return compressFile;
    }

    /**
     * 递归压缩
     * @param zos
     * @param file
     * @param baseDir
     */
    private void compressZip0(ZipOutputStream zos, File file, String baseDir){
        // 压缩文件缓冲区大小
        FileInputStream in = null;
        if(file.isFile()){
            try {
                // 生成下一个压缩节点
                zos.putNextEntry(new ZipEntry(baseDir));
            } catch (IOException e1) {
                log.warn(e1);
            }
            byte[] buffere = new byte[4096];
            int length = 0;// 读取的长度
            try {
                in = new FileInputStream(file);
                while ((length = in.read(buffere)) != -1) {
                    zos.write(buffere, 0, length);
                }
            } catch (IOException e) {
                log.error(e);
            } finally {
                // 当压缩完成,关闭流
                if(in != null){
                    try{
                        in.close();
                    } catch(IOException e){
                        log.debug(e);
                    }
                }
            }
            log.debug("压缩文件:  " + file.getAbsolutePath() + "  成功!");
            return ;
        } else {
            try {
                zos.putNextEntry(new ZipEntry(baseDir + "/"));
            } catch (IOException e) {
                log.warn(e);
            }

            baseDir = baseDir.length() == 0 ? "" : (baseDir + "/");
            File[] files = file.listFiles();
            // 遍历所有文件,逐个进行压缩
            for(File f : files){
                compressZip0(zos, f, baseDir + f.getName());
            }
        }
    }



    /**
     * 解压缩指定zip文件名.
     * @param zipFile zip文件名
     * @param uncompressPath 解压说的后要保存的文件路径,若没有,系统自动新建
     * @throws IOException 解压缩异常
     */
    public void uncompressZip(String zipFile, String uncompressPath)
            throws IOException {
        uncompressZip(new File(zipFile), new File(uncompressPath));
    }


    /**
     * 解压缩指定zip文件.
     *
     * @param zipFile
     *            zip文件名
     * @param uncompressPath
     *            解压说的后要保存的文件路径,若没有,系统自动新建
     * @throws IOException 解压缩异常
     */
    public void uncompressZip(File zipFile, File uncompressPath)
            throws IOException {
        ZipFile zip = null;// 创建解压缩文件
        try {
            zip = new ZipFile(zipFile);
        } catch (IOException e) {
            log.error(e);
            return;
        }
        // 如果指定的路径不存在,创建文件夹
        if (!uncompressPath.exists()) {
            if (!uncompressPath.mkdirs()) {
                StringBuilder exception = new StringBuilder();
                exception.append(uncompressPath);
                exception.append("路径不可到达,并且解压缩");
                exception.append(zipFile);
                exception.append("失败!");
                throw new IOException(exception.toString());
            }
        }
        // 返回 ZIP 文件条目的枚举。
        Enumeration<? extends ZipEntry> en = zip.entries();
        ZipEntry entry = null;

        File file = null;
        // 遍历每一个文件
        while (en.hasMoreElements()) {
            // 如果压缩包还有下一个文件,则循环
            entry = (ZipEntry) en.nextElement();
            if (entry.isDirectory()) {
                // 如果是文件夹,创建文件夹并加速循环
                file = new File(uncompressPath, entry.getName());
                file.mkdirs();
                continue;
            }
            // 构建文件对象
            file = new File(uncompressPath, entry.getName());
            if (!file.getParentFile().exists()) {
                // 如果文件对象的父目录不存在,创建文件夹
                if(!file.getParentFile().mkdirs()){
                    log.debug("can not create dir: " + file.getAbsolutePath());
                }
            }

            InputStream in = null;
            FileOutputStream out = null;
            try {
                in = zip.getInputStream(entry);
                out = new FileOutputStream(file);
                byte[] bytes = new byte[2048];
                int size = -1;
                while((size = in.read(bytes)) != -1){
                     out.write(bytes, 0, size);
                }
                out.flush();
            } catch (IOException e) {
                log.warn(e);
            } finally {
                if(in != null){
                    try {
                        in.close();
                    }catch(IOException e) {
                        log.debug(e);
                    }
                }
                if(out != null){
                    try {
                        out.close();
                    }catch(IOException e) {
                        log.debug(e);
                    }
                }
            }
            log.debug("解压文件:" + entry.getName() + "成功!");
        }
        try{
            zip.close();
        } catch(IOException e) {
            log.debug(e);
        }
    }



    /**
     * 递归获得该文件夹下所有文件(不包括目录).
     * 如果该文件路径指向一个文件,则返回该文件的单个集合,
     * 如果该文件指向一个目录,则返回该目录下的所有文件
     *
     * @param file
     *            可以是目录,也可以是文件,当是文件时,直接返回该文件的列表
     * @return 返回该文件夹下的所有子文件
     */
    public List<File> getSubFile(File file) {
        // 文件列表对象
        List<File> fileList = new ArrayList<File>();
        if (file.isFile()) {
            // 如果是普通文件,直接把该文件添加到文件列表
            fileList.add(file);
            return fileList;
        }
        if (file.isDirectory()) {
            fileList.add(file);
            // 如果是目录,则遍历目录下的所有文件
            for (File f : file.listFiles()) {
                // 这里使用的递归,一直到文件目录的最底层,而文件列表里存的,全是文件
                fileList.addAll(getSubFile(f));
            }
        }
        return fileList;
    }

}
