package com.bigdata.elasticsearch.util;

import com.bigdata.elasticsearch.criterion.builder.ESAggregationCriterionBuilder;
import com.bigdata.elasticsearch.criterion.builder.ESQueryCriterionBuilder;
import com.bigdata.elasticsearch.criterion.manager.ESQueryCriterionBuilderManager;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@SpringBootTest()
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {EsUtil.class})
@TestPropertySource(locations = {"classpath:es-config.properties"})
class EsUtilTest {

    @Autowired
    EsUtil esUtil;

    @AfterEach
    public void close() {
        esUtil.close();
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

        try {
            esUtil.insertDocument("app_account1", "blog", "1", json);
        } catch (Exception e) {
            e.printStackTrace();
        }
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
        esUtil.insertDocument("app_account2", "doc", "1", doc);
    }

    @Test
    public void bulkInsertData1() {
        Map<String, String> map = new HashMap<>();
        map.put("12", "{" +
                "\"account_id\":\"12\"," +
                "\"account_title\":\"abcd\"," +
                "\"content\":\"abcdefg。\"," +
                "\"postdate\":\"2019-11-25 14:38:00\"," +
                "\"url\":\"baidu.com\"" +
                "}");
        map.put("13", "{" +
                "\"account_id\":\"13\"," +
                "\"account_title\":\"abcd\"," +
                "\"content\":\"abcdefg。\"," +
                "\"postdate\":\"2019-11-25 14:38:00\"," +
                "\"url\":\"baidu.com\"" +
                "}");
        esUtil.bulkInsertData("app_account1", "blog", map);
        System.out.println(map);
    }

    @Test
    public void bulkInsertData2() {
        List<String> jsonList = new ArrayList<>();
        jsonList.add("{" +
                "\"account_id\": 10 ," +
                "\"account_title\":\"Java插入数据到ES\"," +
                "\"content\":\"abcdefg。\"," +
                "\"postdate\":\"2019-11-25 14:38:00\"," +
                "\"url\":\"baidu.com\"" +
                "}");
        jsonList.add("{" +
                "\"account_id\": 11," +
                "\"account_title\":\"Java插入数据到ES\"," +
                "\"content\":\"abcdefg。\"," +
                "\"postdate\":\"2019-11-25 14:38:00\"," +
                "\"url\":\"baidu.com\"" +
                "}");
        esUtil.bulkInsertData("app_account1", "blog", jsonList);
        System.out.println(jsonList);
    }


    @Test
    public void deleteIndex() {
        boolean app_account = esUtil.deleteIndex("app_account2");
        System.out.println(app_account);
    }


    @Test
    public void deleteDocument() {
        int i = esUtil.deleteByDocumentId("app_account1", "blog", "1");
        System.out.println(i);
    }

    @Test
    public void updateDocument1() throws IOException, ExecutionException, InterruptedException {
        XContentBuilder doc = jsonBuilder()
                .startObject()
                .field("account_id", "22")
                .field("account_title", "Java插入数据到ES")
                .field("content", "abcedfasdasd")
                .field("postdate", "2019-11-25 14:38:00")
                .field("url", "baidu.com")
                .endObject();
        int i = esUtil.OnlyUpdateByDocumentId("app_account1", "_doc", "1", doc);
        System.out.println(i);
    }

    @Test
    public void upsertDocument2() throws IOException, ExecutionException, InterruptedException {
        XContentBuilder doc = jsonBuilder()
                .startObject()
                .field("account_id", "4444")
                .field("account_title", "Java插入数据到ES")
                .field("content", "abcedfasdasd")
                .field("postdate", "2019-11-25 14:38:00")
                .field("url", "baidu.com")
                .endObject();
        int i = esUtil.upsertInsertByDocumentId("app_account1", "blog", "1", doc, doc);
        System.out.println(i);
    }

    @Test
    public void selectDocument() {
        String app_account = esUtil.selectByDocumentId("app_account1", "blog", "1");
        System.out.println(app_account);
    }

    @Test
    public void search() {
        ESQueryCriterionBuilderManager manager = new ESQueryCriterionBuilderManager();
        manager.must(new ESQueryCriterionBuilder().fuzzy("account_title", "数").term("account_id", "11"));
//        manager.should(new ESQueryCriterionBuilder().queryString("abcedfasdasd OR java"))
//                .must(new ESQueryCriterionBuilder().range("account_id", 10, 20))
//                .mustNot(new ESQueryCriterionBuilder().term("account_id", "11"));
        manager.setSize(10);  //查询返回条数，最大 10000
        manager.setFrom(0);  //分页查询条目起始位置， 默认0
        manager.setDesc("account_id"); //排序

        esUtil.search("app_account1", "blog", manager).forEach(System.out::println);
//        List<Map<String, Object>> app_account = esUtil.search("app_account1", "blog", manager);
    }

    @Test
    public void statSearch() {

        ESQueryCriterionBuilderManager manager = new ESQueryCriterionBuilderManager();
        manager.should(new ESQueryCriterionBuilder().range("account_id", 10, 20));
        manager.setSize(10);  //查询返回条数，最大 10000
        manager.setFrom(0);  //分页查询条目起始位置， 默认0
        manager.setDesc("account_id"); //排序

        esUtil.statSearch("app_account1", "blog", manager, "content").forEach((k, v) -> System.out.println(k + "--->>>" + v));
    }

    @Test
    public void statSearch2() {

        ESQueryCriterionBuilderManager manager = new ESQueryCriterionBuilderManager();
        manager.should(new ESQueryCriterionBuilder().range("account_id", 10, 11));
        manager.setSize(10);  //查询返回条数，最大 10000
        manager.setFrom(0);  //分页查询条目起始位置， 默认0
        manager.setDesc("account_id"); //排序


        ESAggregationCriterionBuilder agg = new ESAggregationCriterionBuilder()
                .group("content")
                .sum("account_id");

        esUtil.statSearch("app_account1", "blog", manager, agg, "content")
                .forEach((k, v) -> System.out.println(k + "--->>>" + v));

    }

}