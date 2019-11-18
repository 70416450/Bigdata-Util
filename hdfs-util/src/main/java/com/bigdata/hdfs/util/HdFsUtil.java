package com.bigdata.hdfs.util;


import com.bigdata.hdfs.conf.HdFsConnection;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Component
public class HdFsUtil {

//    @Value("${server.host}")
//    private String serverHost;
//
//    @Value("${hdfs.fs.defaultFS}")
//    private String host;
//
//    @Value("${hdfs.dfs.nameservices}")
//    private String regex;

    @Autowired
    private HdFsConnection hdFsConnection;

    /**
     * @param [sourcePath-->物理机源路径, targetPath-->hdfs目标路径 ]
     * @return java.lang.String 目标路径
     * @describe 将物理机文件存入HdFs文件系统
     */
    public String localDataToHdfs(String sourcePath, String targetPath) {

        FileSystem fs = hdFsConnection.getFSConnection();

        try {
            fs.copyFromLocalFile(new Path(sourcePath), new Path(targetPath));
        } catch (IOException e) {
            log.error("文件存入HdFs文件系统失败");
            e.printStackTrace();
        } finally {
            HdFsConnection.close(fs);
        }

        return targetPath;
    }

    /**
     * @param [sourcePath-->Hdfs路径, targetPath-->hdfs物理机源路径 ]
     * @return java.lang.String 目标路径
     * @describe 将HdFs文件系统文件存入物理机
     */
    public String hdfsToLocalData(String sourcePath, String targetPath) {

        FileSystem fs = hdFsConnection.getFSConnection();

        try {
            fs.copyToLocalFile(new Path(sourcePath), new Path(targetPath));
        } catch (IOException e) {
            log.error("文件存入HdFs文件系统失败");
            e.printStackTrace();
        } finally {
            HdFsConnection.close(fs);
        }

        return targetPath;
    }


    /**
     * @param [hdfsFilePath-->hdfs目标路径 ]
     * @return java.lang.String 文件内容
     * @describe 根据文件在HdFs文件系统上所处路径查询文件内容
     */
    public String getFileContent(String hdfsFilePath) {
        StringBuffer sb = null;
        try (FSDataInputStream fds = hdFsConnection.getFDSConnection(hdfsFilePath)) {
            sb = new StringBuffer();
            byte[] buff = new byte[1024];
            int length = 0;
            while ((length = fds.read(buff)) != -1) {
                sb.append(new String(buff, 0, length));
            }
            log.info("查询文件信息成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    /**
     * @param [hdfsFilePath-->hdfs目标路径]
     * @return boolean
     * @describe 创建目录，主要用于创建一个目录
     */
    public boolean createDirectory(String hdfsFilePath) {
        FileSystem fs = hdFsConnection.getFSConnection();
        try {
            Path path = new Path(hdfsFilePath);
            if (fs.exists(path)) {
                log.info("文件夹已经存在");
                return true;
            }
            fs.mkdirs(path);
        } catch (IOException e) {
            log.error("创建目录失败");
            e.printStackTrace();
        } finally {
            HdFsConnection.close(fs);
        }
        return true;
    }

    /**
     * @param [hdfsFilePath-->hdfs目标路径 ]
     * @return java.util.List
     * @describe 获取文件夹下的文件及文件夹列表(支持一级)
     */
    public List getFileAndDirectory(String hdfsFilePath) {

        FileSystem fs = hdFsConnection.getFSConnection();
        Path path = new Path(hdfsFilePath);
        try {
            List<String> list = new ArrayList<>();

            // 获取文件列表
            if (fs.exists(path)) {
                for (FileStatus status : fs.listStatus(path)) {
                    if (status.isDirectory()) {
                        list.add("Directory:" + status.getPath().toString().split(hdFsConnection.getNameServices())[1]);
                    } else {
                        list.add("File:" + status.getPath().toString().split(hdFsConnection.getNameServices())[1]);
                    }
                }
            }
            return list;
        } catch (IOException e) {
            log.error("获取文件列表失败");
            e.printStackTrace();
        } finally {
            HdFsConnection.close(fs);
        }
        return null;
    }

    /**
     * @param [hdfsFilePath-->hdfs目标路径 ]
     * @return boolean
     * @describe 删除文件或文件夹
     */
    public boolean delete(String hdfsFilePath) {
        FileSystem fs = hdFsConnection.getFSConnection();
        try {
            fs.delete(new Path(hdfsFilePath), true);
        } catch (IOException e) {
            log.error("删除文件或文件夹失败");
            e.printStackTrace();
        } finally {
            HdFsConnection.close(fs);
        }
        return true;
    }

    /**
    * @param [hdfsFilePath-->hdfs目标路径 ]
    * @return long 
    * @describe 获取文件大小
    */
    public long getFileSize(String hdfsFilePath) {
        long size = 1L;
        FileSystem fs = hdFsConnection.getFSConnection();

        try {
            size = fs.getContentSummary(new Path(hdfsFilePath)).getLength();
        } catch (IOException e) {
            log.error("获取文件大小失败");
            e.printStackTrace();
        } finally {
            HdFsConnection.close(fs);
        }
        return size;
    }


    /**
    * @param [srcPath-->hdfs目标原名称 , destPath-->hdfs目标新名称]
    * @return boolean 
    * @describe 文件重命名
    */
    public boolean fileRename(String srcPath, String destPath) {
        FileSystem fs = hdFsConnection.getFSConnection();
        try {
            boolean result = fs.rename(new Path(srcPath), new Path(destPath));
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
    * @param [srcPath-->hdfs目标原名称, destPath-->hdfs目标新名称]
    * @return boolean
    * @describe 将其他用户的文件转储在自己的账户
    */
    public boolean storeFileFromOthers(String srcPath, String destPath) {
        FileSystem fs = hdFsConnection.getFSConnection();
        FSDataInputStream srcStream = null;
        FSDataOutputStream destStream = null;
        try {
            srcStream = fs.open(new Path(srcPath));
            destStream = fs.create(new Path(destPath), true);

            IOUtils.copyBytes(srcStream, destStream, 1024, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(srcStream);
            IOUtils.closeStream(destStream);
            HdFsConnection.close(fs);
        }
        return true;
    }

    /**
    * @param [srcPath-->hdfs目标原名称, destPath-->hdfs目标新名称]
    * @return boolean 
    * @describe 将其他用户的文件或者文件夹转储在自己的账户
    */
    public boolean storeFolderFromOthers(String srcPath, String destPath) {
        FileSystem fs = hdFsConnection.getFSConnection();

        try {
            Path path = new Path(srcPath);
            List<String> lastOne = Arrays.asList(srcPath.split("/"));

            String destFilePath = destPath + "/" + lastOne.get(lastOne.size() - 1);

            // 判断是文件夹操作还是文件操作
            if (fs.getFileStatus(path).isDirectory()) {
                createDirectory(destFilePath);
                // 遍历文件夹中的文件
                for (FileStatus status : fs.listStatus(path)) {
                    String srcFilePath = status.getPath().toString().split(hdFsConnection.getNameServices())[1];
                    // 递归文件夹，创建文件夹或者存储文件
                    if (status.isDirectory()) {
                        storeFolderFromOthers(srcFilePath, destFilePath);
                    } else {
                        List<String> last = Arrays.asList(srcFilePath.split("/"));
                        String copyFilePath = destFilePath + "/" + last.get(last.size() - 1);
                        storeFileFromOthers(srcFilePath, copyFilePath);
                    }
                }
            } else {
                storeFileFromOthers(srcPath, destFilePath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HdFsConnection.close(fs);
        }

        return true;
    }

    /**
    * @param [srcPath, request]
    * @return com.bigdata.hdfs.util.HdFsUtil.ResumeBreakPoint
    * @describe 主要用于下载的断点续传，获取前端传递的range
    */
    public ResumeBreakPoint getRange(String srcPath, HttpServletRequest request) {
        long startByte = 0;
        long endByte = getFileSize(srcPath) - 1;
        String range = request.getHeader("Content-Range");
        ResumeBreakPoint resume = new ResumeBreakPoint();

        if (range != null && range.contains("bytes=") && range.contains("-")) {
            range = range.substring(range.lastIndexOf("=") + 1).trim();
            String[] ranges = range.split("-");
            try {
                // 判断range类型
                if (ranges.length == 1) {
                    if (range.startsWith("-")) {
                        endByte = Long.parseLong(ranges[0]);
                    } else if (range.endsWith("-")) {
                        startByte = Long.parseLong(ranges[0]);
                    }
                } else if (ranges.length == 2) {
                    startByte = Long.parseLong(ranges[0]);
                    endByte = Long.parseLong(ranges[1]);
                }

            } catch (NumberFormatException e) {
                startByte = 0;
                endByte = getFileSize(srcPath) - 1;
            }
        }

        // 要下载的长度
        long contentLength = endByte - startByte + 1;
        resume.setStartByte(startByte);
        resume.setEndByte(endByte);
        resume.setContentLength(contentLength);
        return resume;
    }

    /** 
    * @param [srcPath, request, response]
    * @return boolean 
    * @describe 从HdFs文件系统下载文件至发送请求的用户
    */
    public boolean downloadFile(String srcPath, HttpServletRequest request, HttpServletResponse response) throws UnsupportedEncodingException {

        FSDataInputStream fds = hdFsConnection.getFDSConnection(srcPath);
        ResumeBreakPoint resume = getRange(srcPath, request);

        // 请求参数
        String queryString = request.getQueryString();

        // 文件名
        String newName = queryString.split("=")[1];
        // 文件类型
//        String contentType = request.getServletContext().getMimeType(newName);

        response.setHeader("Accept-Ranges", "bytes");
        // 状态码设为206
        response.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT);
//        response.setHeader("Content-Type", contentType);
        response.setHeader("Content-Length", String.valueOf(resume.getContentLength()));
        response.setHeader("Content-Range", "bytes " + resume.getStartByte() + "-" + resume.getEndByte() + "/" + getFileSize(srcPath));

        // 清除首部的空白行，必须加上这句，不然无法在chrome中使用,另外放在Content-Disposition属性之前，不然无法设置文件名
        response.reset();
        response.setHeader("Content-Disposition", "attachment; filename=\"" + newName + "\"");

        // 已传送数据大小
        long transmitted = 0;
        try {
            long contentLength = resume.getContentLength();
            int length = 0;
            byte[] buf = new byte[4096];
            OutputStream os = response.getOutputStream();

            while ((transmitted + length) <= contentLength && (length = fds.read(buf)) != -1) {
                os.write(buf);
                transmitted += length;
            }
            //处理不足buff.length部分
            if (transmitted < contentLength) {
                length = fds.read(buf, 0, (int) (contentLength - transmitted));
                os.write(buf, 0, length);
                transmitted += length;
            }
            os.close();
            fds.close();
            System.out.println("下载完毕：" + resume.getStartByte() + "-" + resume.getEndByte() + "：" + transmitted);
        } catch (IOException e) {
            log.error("用户停止下载" + newName + "文件");
        }
        return true;
    }

    /**
     * 描述：该类用于存储被下载文件的起始位置和结束位置
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private class ResumeBreakPoint {
        long startByte;
        long endByte;
        long contentLength;
    }
}
