package com.bigdata.hdfs.conf;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;

/**
 * @author Heaton
 * @email tzytzy70416450@gmail.com
 * @date 2019/11/15 17:16
 * @describe HdFs的连接操作
 */
@Slf4j
@Getter
@Component
public class HdFsConnection {

    @Value("${hdfs.user}")
    private String user;

    @Value("${hdfs.ha}")
    private boolean ha;

    @Value("${hdfs.fs.defaultFS}")
    private String defaultFS;

    @Value("${hdfs.fs.hdfs.impl}")
    private String hdfsImpl;

    @Value("${hdfs.dfs.nameservices}")
    private String nameServices;

    @Value("${hdfs.dfs.ha.namenodes}")
    private String nameNodes;

    /**
     * @param []
     * @return org.apache.hadoop.conf.Configuration
     * @describe 初始化HdFs的配置Configuration
     */
    public Configuration init() {
        Configuration con = new Configuration();
        con.set("fs.defaultFS", defaultFS);
        con.set("fs.hdfs.impl", hdfsImpl);
        if (ha) {
            con.set("dfs.nameservices", nameServices);
            con.set("dfs.client.failover.proxy.provider." + nameServices, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
            String[] list = nameNodes.split(",");
            String nn = "";
            for (int i = 1; i <= list.length; i++) {
                nn += "nn" + i + ",";
                con.set("dfs.namenode.rpc-address." + nameServices + ".nn" + i, list[i - 1]);
            }
            con.set("dfs.ha.namenodes." + nameServices, nn.substring(0, nn.length() - 1));

        }
        return con;
    }


    /**
     * @param [path] HDFS文件路径
     * @return org.apache.hadoop.fs.FileSystem
     * @describe 获取一个FileSystem实例(系统实例)
     */
    public FileSystem getFSConnection(String path) {

        String resourcePath = defaultFS + path;
        FileSystem fs = null;
        Configuration con = init();

        try {
            fs = FileSystem.get(new URI(resourcePath), con, user);
        } catch (Exception e) {
            log.error("连接hdfs文件系统失败");
            e.printStackTrace();
        }

        return fs;
    }


    /**
     * @param []
     * @return org.apache.hadoop.fs.FileSystem
     * @describe 获取一个FileSystem实例(系统实例)
     */
    public FileSystem getFSConnection() {
        return getFSConnection("");
    }


    /**
     * @param [path] HDFS文件路径
     * @return org.apache.hadoop.fs.FSDataInputStream
     * @describe 获取一个FSDataInputStream实例(流对象)
     */
    public FSDataInputStream getFDSConnection(String path) {

        String resourcePath = defaultFS + path;
        FileSystem fs;
        FSDataInputStream fds = null;
        try {
            fs = getFSConnection(path);
            fds = fs.open(new Path(resourcePath));
        } catch (Exception e) {
            log.error("连接hdfs文件系统失败");
            e.printStackTrace();
        }
        return fds;
    }

    /**
     * @param [fs] 文件系统
     * @return void
     * @describe 关闭FileSystem连接
     */
    public static void close(FileSystem fs) {
        try {
            fs.close();
        } catch (IOException e) {
            log.error("关闭FileSystem连接失败");
            e.printStackTrace();
        }
    }

    /**
     * @param [fds] 文件流
     * @return void
     * @describe 关闭FSDataInputStream连接
     */
    public static void close(FSDataInputStream fds) {
        try {
            fds.close();
        } catch (IOException e) {
            log.error("关闭FSDataInputStream连接失败");
            e.printStackTrace();
        }
    }
}
