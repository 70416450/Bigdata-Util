package com.bigdata.hdfs.util;

import com.bigdata.hdfs.conf.HdFsConnection;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@SpringBootTest()
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {HdFsConnection.class, HdFsUtil.class})
@TestPropertySource(locations = {"classpath:hdfs.properties"})
class HdFsUtilTest {
    @Autowired
    private HdFsUtil hdFsUtil;

    @Test
    void storeInData() {
        String s = hdFsUtil.storeInData("C:\\Users\\SMZC\\Desktop\\core-site.xml", "/myTest/core-site.xml");
        System.out.println(s);
    }
}