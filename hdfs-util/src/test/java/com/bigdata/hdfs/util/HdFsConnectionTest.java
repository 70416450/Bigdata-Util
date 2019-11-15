package com.bigdata.hdfs.util;

import com.bigdata.hdfs.conf.HdFsConnection;
import org.apache.hadoop.fs.FSDataInputStream;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

@SpringBootTest()
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {HdFsConnection.class, HdFsUtil.class})
@TestPropertySource(locations = {"classpath:hdfs.properties"})
class HdFsConnectionTest {

    @Autowired
    private HdFsConnection hdFsConnection;

    @Test
    void getFSConnection() {
        hdFsConnection.getFSConnection();
    }

    @Test
    void getFSConnection1() throws IOException {
        FSDataInputStream fds = hdFsConnection.getFDSConnection("/test/core-site.xml");
        byte[] buff = new byte[1024];
        int length = 0;

        while ((length = fds.read(buff)) != -1) {
            System.out.println(new String(buff, 0, length));
        }
        System.out.println("查询文件信息成功");
    }


}