package com.bigdata.hdfs.util;

import com.bigdata.hdfs.conf.HdFsConnection;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@SpringBootTest()
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {HdFsConnection.class})
@TestPropertySource(locations = {"classpath:hdfs.properties"})
class HdFsConnectionTest {

    @Autowired
    private HdFsConnection hdFsConnection;

    @Test
    void getFSConnection() {
        FileSystem fs = hdFsConnection.getFSConnection();
        System.out.println(fs);
        HdFsConnection.close(fs);
    }

    @Test
    void getFSConnection1() {
        FileSystem fs = hdFsConnection.getFSConnection("/");
        System.out.println(fs);
        HdFsConnection.close(fs);
    }

    @Test
    void getFSConnection3() {
        FSDataInputStream fds = hdFsConnection.getFDSConnection("/aa.txt");
        System.out.println(fds);
        HdFsConnection.close(fds);
    }

}