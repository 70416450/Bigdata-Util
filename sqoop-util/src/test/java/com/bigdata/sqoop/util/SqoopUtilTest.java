package com.bigdata.sqoop.util;


import com.bigdata.shell.conf.ShellConfig;
import com.bigdata.shell.util.ShellUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@SpringBootTest()
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ShellConfig.class})
@TestPropertySource(locations = {"classpath:shell-config.properties"})
@Slf4j
public class SqoopUtilTest {

    @Autowired
    ShellUtil shellUtil;

    @Test
    public void test1() {
        shellUtil.login("exec");
        String cmd = "sqoop import " +
                "--connect jdbc:mysql://10.28.17.238:3306/bigdata_new " +
                "--username root " +
                "--password smcdyanfa " +
                "--table pb_test " +
                "--target-dir /myTest/bigdata_new " +
                "--delete-target-dir " +
                "--num-mappers 1 " +
                "--fields-terminated-by '\t' ";
        int s = 0;
        try {
            s = shellUtil.execCmd(cmd);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(cmd);

        System.out.println("-->>  " + s);
        shellUtil.logout();
    }
}
