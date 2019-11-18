package com.bigdata.shell.conf;

import com.bigdata.shell.util.ShellUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ShellConfig {
    /**
     * SFTP服务器IP地址
     */
    @Value("${shell.sftp.host}")
    private String host;
    /**
     * SFTP服务器端口
     */
    @Value("${shell.sftp.port}")
    private int port;
    /**
     * 连接超时时间，单位毫秒
     */
    @Value("${shell.sftp.timeout}")
    private int timeout;
    /**
     * 用户名
     */
    @Value("${shell.sftp.username}")
    private String username;
    /**
     * 密码
     */
    @Value("${shell.sftp.password}")
    private String password;

    @Bean
    public ShellUtil shellUtil() {
        return new ShellUtil(host, port, timeout, username, password);
    }
}
