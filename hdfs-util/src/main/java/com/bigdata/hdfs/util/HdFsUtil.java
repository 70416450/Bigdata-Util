package com.bigdata.hdfs.util;


import com.bigdata.hdfs.conf.HdFsConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
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
}
