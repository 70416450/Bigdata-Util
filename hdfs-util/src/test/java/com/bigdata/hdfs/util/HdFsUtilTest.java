package com.bigdata.hdfs.util;

import com.bigdata.hdfs.conf.HdFsConnection;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.UnsupportedEncodingException;
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
        String s = hdFsUtil.localDataToHdfs("D:\\cc.txt", "/myTest/cc.txt");
        System.out.println(s);
    }

    @Test
    void hdfsToLocalData() {
        String s = hdFsUtil.hdfsToLocalData("/myTest/cc.txt", "D:\\dd.txt");
        System.out.println(s);
    }


    @Test
    void getFileContent() {
        String s = hdFsUtil.getFileContent("/myTest/cc.txt");
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
        boolean fileRename = hdFsUtil.fileRename("/myTest/cc.txt", "/myTest/dd.txt");
        System.out.println(fileRename);
    }

    @Test
    void storeFileFromOthers() {
        boolean storeFileFromOthers = hdFsUtil.storeFileFromOthers("/aa.txt", "/myTest/ee.txt");
        System.out.println(storeFileFromOthers);
    }


    @Test
    void storeFolderFromOthers() {
        boolean b = hdFsUtil.storeFolderFromOthers("/myTest", "/myTest1/myTest2/myTest3");
        System.out.println(b);
    }


    private MockHttpServletRequest request;
    private MockHttpServletResponse response;

    @Test
    void downloadFile() throws UnsupportedEncodingException {
        request = new MockHttpServletRequest();
        request.setCharacterEncoding("UTF-8");
        request.setQueryString("=/myTest/dd.txt");
        response = new MockHttpServletResponse();
        boolean b = hdFsUtil.downloadFile("/myTest/dd.txt", request, response);
        System.out.println(b);
    }
}