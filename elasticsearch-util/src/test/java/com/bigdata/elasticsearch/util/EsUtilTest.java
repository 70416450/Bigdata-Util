package com.bigdata.elasticsearch.util;


import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@SpringBootTest()
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {EsUtil.class})
@TestPropertySource(locations = {"classpath:es-config.properties"})
class EsUtilTest {

    @Autowired
    EsUtil esUtil;

    @Test
    public void insertDocument1() {
        // 方式一
        String json = "{" +
                "\"id\":\"1\"," +
                "\"title\":\"Java插入数据到ES\"," +
                "\"content\":\"abcdefg。\"," +
                "\"postdate\":\"2019-11-25 14:38:00\"," +
                "\"url\":\"baidu.com\"" +
                "}";
        System.out.println(json);

        esUtil.insertDocument("app_account", "blog", json);
    }

    @Test
    public void insertDocument2() throws IOException {
        // 方式二
        XContentBuilder doc = jsonBuilder()
                .startObject()
                .field("id", "2")
                .field("title", "Java插入数据到ES")
                .field("content", "abcedfasdasd")
                .field("postdate", "2019-11-25 14:38:00")
                .field("url", "baidu.com")
                .endObject();
        esUtil.insertDocument("app_account", "doc", doc);
    }


    @Test
    public void isExists() {
        boolean app_account = esUtil.isExists("app_account");
        System.out.println(app_account);
    }

    @Test
    public void createIndex1() {
        boolean app_account = esUtil.createIndex("app_account1");
        System.out.println(app_account);
    }

    @Test
    public void createIndex2() {
        boolean app_account = esUtil.createIndex("app_account2", 2, 2);
        System.out.println(app_account);
    }

    @Test
    public void deleteIndex() {
        boolean app_account = esUtil.deleteIndex("app_account2");
        System.out.println(app_account);
    }

    @Test
    public void selectDocument() {
        String app_account = esUtil.selectDocument("app_account", "blog", "zG6DoW4ByGAgmkNPGdvc");
        System.out.println(app_account);
    }

    @Test
    public void deleteDocument() {
        int i = esUtil.deleteDocument("app_account", "blog", "zG6DoW4ByGAgmkNPGdvc");
        System.out.println(i);
    }

    @Test
    public void updateDocument1() throws IOException, ExecutionException, InterruptedException {
        XContentBuilder doc = jsonBuilder()
                .startObject()
                .field("id", "22")
                .field("title", "Java插入数据到ES")
                .field("content", "abcedfasdasd")
                .field("postdate", "2019-11-25 14:38:00")
                .field("url", "baidu.com")
                .endObject();
        int i = esUtil.updateDocument("app_account", "blog", "zW6DoW4ByGAgmkNPGtuA1", doc);
        System.out.println(i);
    }

    @Test
    public void upsertDocument() throws IOException, ExecutionException, InterruptedException {
        XContentBuilder doc = jsonBuilder()
                .startObject()
                .field("id", "3333")
                .field("title", "Java插入数据到ES")
                .field("content", "abcedfasdasd")
                .field("postdate", "2019-11-25 14:38:00")
                .field("url", "baidu.com")
                .endObject();
        int i = esUtil.upsertDocument("app_account", "blog", "zW6DoW4ByGAgmkNPGtuA43", doc, doc);
        System.out.println(i);
    }


}