package com.bigdata.hdfs.util;


import com.bigdata.hdfs.conf.HdFsConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

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
    * @param [sourcePath-->源路径, targetPath-->目标路径 ] 
    * @return java.lang.String 目标路径
    * @describe 将文件存入HdFs文件系统
    */
    public String storeInData(String sourcePath, String targetPath) {

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




}
