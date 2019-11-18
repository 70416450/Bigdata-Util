package com.bigdata.hdfs.util;

import com.bigdata.hdfs.conf.HdFsConnection;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

@SpringBootTest()
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {HdFsConnection.class, HdFsUtil.class})
@TestPropertySource(locations = {"classpath:hdfs.properties"})
class HdFsUtilTest {
    @Autowired
    private HdFsUtil hdFsUtil;

    @Test
    void localDataToHdfs() {
        String s = hdFsUtil.localDataToHdfs("D:\\core-site.xml", "/myTest/core-site.xml");
        System.out.println(s);
    }

    @Test
    void hdfsToLocalData() {
        String s = hdFsUtil.hdfsToLocalData("/myTest/core-site.xml", "D:\\core-site1.xml");
        System.out.println(s);
    }


    @Test
    void getFileContent() {
        String s = hdFsUtil.getFileContent("/myTest/core-site.xml");
        System.out.println(s);
    }

    @Test
    void createDirectory() {
        boolean directory = hdFsUtil.createDirectory("/myTest/aa");
        System.out.println(directory);
    }

    @Test
    void getFileAndDirectory() {
        List fileAndFolder = hdFsUtil.getFileAndDirectory("/myTest");
        System.out.println(fileAndFolder);
    }


    @Test
    void delete() {
        boolean delete = hdFsUtil.delete("/myTest/aa");
        System.out.println(delete);
    }


    @Test
    void getFileSize() {
        long fileSize = hdFsUtil.getFileSize("/myTest");
        System.out.println(fileSize + "b");
    }


    @Test
    void fileRename() {
        boolean fileRename = hdFsUtil.fileRename("/myTest/core-site.xml","/myTest/core-site1.xml");
        System.out.println(fileRename);
    }



}