package com.bigdata.shell.util;


import com.bigdata.shell.conf.ShellConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;


@SpringBootTest()
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ShellConfig.class})
@TestPropertySource(locations = {"classpath:shell-config.properties"})
class ShellUtilTest {

    @Autowired
    private ShellUtil shellUtil;

//    @BeforeEach
//    void login() {
//        shellUtil.login("sftp");
//    }
//
//    @AfterEach
//    void logout() {
//        shellUtil.logout();
//    }

    //sftp模式测试
    @Test
    void makeDir() {
        boolean b = shellUtil.makeDir("/abc");
        System.out.println(b);
    }

    @Test
    void uploadFile() {
        boolean b = shellUtil.uploadFile("/abc", "bb.txt", "D:\\aa.txt");
        System.out.println(b);
    }

    @Test
    void downloadFile() {
        boolean b = shellUtil.downloadFile("/abc", "bb.txt", "D:\\");
        System.out.println(b);
    }

    @Test
    void delDir() {
        boolean b = shellUtil.delDir("/abc");
        System.out.println(b);
    }

    @Test
    void delFile() {
        boolean b = shellUtil.delFile("/abc/cc.txt");
        System.out.println(b);
    }

    @Test
    void ls() {
        String[] ls = shellUtil.ls();
        Arrays.stream(ls).forEach(System.out::println);
    }

    @Test
    void lsPath() {
        String[] ls = shellUtil.ls("/bin");
        Arrays.stream(ls).forEach(System.out::println);
    }

    //exec模式测试
    @BeforeEach
    void login() {
        shellUtil.login("exec");
    }

    @AfterEach
    void logout() {
        shellUtil.logout();
    }

    @Test
    void execTest() throws Exception {
        String cmd = "cd /" + ";" + "ls -al |grep home";
        String result = shellUtil.execCmd(cmd);
        System.out.println("---->>>"+result);
    }



}