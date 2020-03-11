package com.bigdata.elasticsearch.util;

import com.alibaba.fastjson.JSONObject;
import com.bigdata.elasticsearch.criterion.builder.ESAggregationCriterionBuilder;
import com.bigdata.elasticsearch.criterion.manager.ESQueryCriterionBuilderManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author Heaton
 * @email tzytzy70416450@gmail.com
 * @date 2019/11/15 17:16
 * @describe ES工具类
 */
@SuppressWarnings("all")
@Slf4j
@Component
public class EsUtil {
    //查询条数最大值
    private final static int MAX = 10000;

    //集群名,默认值elasticsearch
    private static String clusterName;

    //ES集群中某个节点
    private static String hostName;

    //连接端口号
    private static int tcpPort;

    //TransportClient对象，用于连接ES集群
    private static volatile TransportClient client;

    public static String getClusterName() {
        return clusterName;
    }

    @Value("${es.cluster.name}")
    public void setClusterName(String clusterName) {
        EsUtil.clusterName = clusterName;
    }

    public static String getHostName() {
        return hostName;
    }

    @Value("${es.host.name}")
    public void setHostName(String hostName) {
        EsUtil.hostName = hostName;
    }

    public static int getTcpPort() {
        return tcpPort;
    }

    @Value("${es.tcp.port}")
    public void setTcpPort(int tcpPort) {
        EsUtil.tcpPort = tcpPort;
    }


    /**
     * @param []
     * @return org.elasticsearch.client.transport.TransportClient
     * @describe 获取连接
     */
    public static TransportClient getClient() {
        if (client == null) {
            synchronized (TransportClient.class) {
                if (client == null) {
                    try {
                        Settings settings = Settings.builder().put("cluster.name", clusterName).build();
                        client = new PreBuiltTransportClient(settings)
                                .addTransportAddress(new TransportAddress(InetAddress.getByName(hostName), tcpPort));
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return client;
    }

    public static void close() {
        if (client != null) {
            client.close();
        }
    }

    /**
     * @param []
     * @return org.elasticsearch.client.IndicesAdminClient
     * @describe 获取索引管理的IndicesAdminClient
     */
    public static IndicesAdminClient getAdminClient() {
        return getClient().admin().indices();
    }

    /**
     * @param [indexName]
     * @return boolean
     * @describe 判定索引是否存在
     */
    public static boolean isExists(String indexName) {
        IndicesExistsResponse response = getAdminClient().prepareExists(indexName).get();
        return response.isExists() ? true : false;
    }

    /**
     * @param [indexName]
     * @return boolean
     * @describe 创建索引
     */
    public static boolean createIndex(String indexName) {
        CreateIndexResponse createIndexResponse = getAdminClient()
                .prepareCreate(indexName.toLowerCase())
                .get();
        return createIndexResponse.isAcknowledged();
    }

    /**
     * @param [indexName->索引名, shards->分片数, replicas->副本数]
     * @return boolean
     * @describe 创建索引(数据分布在几个分片中 ， 每个分片对应几分copy)
     */
    public static boolean createIndex(String indexName, int shards, int replicas) {
        Settings settings = Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.number_of_replicas", replicas)
                .build();
        CreateIndexResponse createIndexResponse = getAdminClient()
                .prepareCreate(indexName.toLowerCase())
                .setSettings(settings)
                .execute().actionGet();
        return createIndexResponse.isAcknowledged();
    }

    /**
     * @param [indexName]
     * @return boolean
     * @describe 删除索引
     */
    public static boolean deleteIndex(String indexName) {
        AcknowledgedResponse acknowledgedResponse = getAdminClient()
                .prepareDelete(indexName.toLowerCase())
                .execute()
                .actionGet();
        return acknowledgedResponse.isAcknowledged();
    }


    /**
     * @param [indexName->索引名, type->类型, _id 文档ID, doc->XContentBuilder形式数据 ]
     * @return void
     * @describe 插入文档（手动ID）
     */
    public static void insertDocument(String indexName, String type, String _id, XContentBuilder doc) {
        IndexResponse response = null;
        IndexRequestBuilder indexRequestBuilder = getClient().prepareIndex(indexName, type).setSource(doc);

        if (_id == null) {
            response = indexRequestBuilder.get();
        } else {
            response = indexRequestBuilder.setId(_id).get();
        }
        if (response.status() != RestStatus.CREATED) {
            log.info("创建失败");
        }
        log.info("创建成功");
    }

    /**
     * @param [indexName->索引名, type->类型, doc->XContentBuilder形式数据 ]
     * @return void
     * @describe 插入文档(自动ID)
     */
    public static void insertDocument(String indexName, String type, XContentBuilder doc) {
        insertDocument(indexName, type, null, doc);
    }

    /**
     * @param [indexName->索引名, type->类型, _id 文档ID, json->Json格式串]
     * @return void
     * @describe 插入文档（手动ID）
     */
    public static void insertDocument(String indexName, String type, String _id, String json) {
        IndexResponse response = null;
        IndexRequestBuilder indexRequestBuilder = getClient().prepareIndex(indexName, type).setSource(json, XContentType.JSON);
        if (_id == null) {
            response = indexRequestBuilder.get();
        } else {
            response = indexRequestBuilder.setId(_id).get();
        }
        if (response.status() != RestStatus.CREATED) {
            log.info("创建失败");
        }
        log.info("创建成功");
    }

    /**
     * @param [indexName->索引名, type->类型, json->Json格式串]
     * @return void
     * @describe 插入文档(自动ID)
     */
    public static void insertDocument(String indexName, String type, String json) {
        insertDocument(indexName, type, null, json);

    }

    /**
     * @param [index->索引名, type->类型, data->(_id 主键, json 数据)]
     * @return void
     * @describe 批量插入数据
     */
    public void bulkInsertData(String index, String type, Map<String, String> data) {
        BulkRequestBuilder bulkRequest = getClient().prepareBulk();
        data.forEach((param1, param2) -> {
            bulkRequest.add(client.prepareIndex(index, type, param1)
                    .setSource(param2, XContentType.JSON)
            );
        });
        BulkResponse bulkResponse = bulkRequest.get();
    }

    /**
     * @param [index->索引名, type->类型, jsonList->json批量数据]
     * @return void
     * @describe 批量插入数据(id自动生成)
     */
    public void bulkInsertData(String index, String type, List<String> jsonList) {
        BulkRequestBuilder bulkRequest = getClient().prepareBulk();
        jsonList.forEach(item -> {
            bulkRequest.add(client.prepareIndex(index, type)
                    .setSource(item, XContentType.JSON)
            );
        });
        BulkResponse bulkResponse = bulkRequest.get();
    }


    /**
     * @param [indexName->索引名, type->类型, id->文档id]
     * @return void
     * @describe 删除文档
     */
    public static int deleteByDocumentId(String indexName, String type, String id) {
        DeleteResponse response = getClient().prepareDelete(indexName, type, id).get();
//        log.info("被删除文档的类型"+response.getType());
//        log.info("被删除文档的ID"+response.getId());
//        log.info("被删除文档的版本信息"+response.getVersion());
        int i = 0;
        RestStatus status = response.status();
        if (status == RestStatus.OK) {
            i = 1;
            log.info("删除成功");
        }
        if (status == RestStatus.NOT_FOUND) {
            log.info("没找到文件");
        }
        return i;
    }


    /**
     * @param [indexName->索引名, type->类型, id->文档id, doc->XContentBuilder]
     * @return void
     * @describe 更新文档
     */
    public static int OnlyUpdateByDocumentId(String indexName, String type, String id, XContentBuilder doc) throws ExecutionException, InterruptedException {
        UpdateRequest request = new UpdateRequest();
        request.index(indexName).type(type).id(id).doc(doc);
        UpdateResponse response = getClient().update(request).get();
//        log.info("被更新文档的类型"+response.getType());
//        log.info("被更新文档的ID"+response.getId());
//        log.info("被更新文档的版本信息"+response.getVersion());
        int i = 0;
        RestStatus status = response.status();
        if (status == RestStatus.OK) {
            i = 1;
            log.info("更新成功");
        }
        if (status == RestStatus.NOT_FOUND) {
            log.info("没找到文件");
        }
        return i;
    }

    /**
     * @param [indexName->索引名, type->类型, id->文档id, insertDoc->新文档, updateDoc->更新文档]
     * @return void
     * @describe 更新数据，存在文档则使用updateDoc，不存在则使用insertDoc
     */
    public static int upsertInsertByDocumentId(String indexName, String type, String id, XContentBuilder insertDoc, XContentBuilder updateDoc) throws ExecutionException, InterruptedException {
        IndexRequest indexRequest = new IndexRequest(indexName, type, id)
                .source(insertDoc);
        UpdateRequest updateRequest = new UpdateRequest(indexName, type, id)
                .doc(updateDoc).upsert(indexRequest);
        UpdateResponse response = getClient().update(updateRequest).get();
//        log.info("被操作文档的类型"+response.getType());
//        log.info("被操作文档的ID"+response.getId());
//        log.info("被操作文档的版本信息"+response.getVersion());
        int i = 0;
        RestStatus status = response.status();
        if (status == RestStatus.OK) {
            i = 1;
            log.info("更新成功");
        }
        if (status == RestStatus.CREATED) {
            i = 2;
            log.info("新建成功");
        }
        if (status == RestStatus.NOT_FOUND) {
            log.info("没找到文件");
        }
        return i;
    }

    /**
     * @param [indexName->索引名, type->类型, id->文档id]
     * @return java.lang.String
     * @describe 查询文档
     */
    public static String selectByDocumentId(String indexName, String type, String id) {
        GetResponse response = getClient().prepareGet(indexName, type, id).get();
//        log.info("是否存在"+response.isExists());
//        log.info("索引名"+response.getIndex());
//        log.info("类型"+response.getType());
//        log.info("文档id"+response.getId());
//        log.info("版本"+response.getVersion());
        return response.getSourceAsString();
    }

    /**
     * @param [index->索引名, type->类型, manager->查询最终构建者]
     * @return java.util.List<java.util.Map < java.lang.String, java.lang.Object>>
     * @describe 普通条件查询
     * 排序字段需要开启fielddata-->举例：
     * PUT app_account1/_mapping
     * {
     * "properties": {
     * "account_id": {
     * "type":     "text",
     * "fielddata": true
     * }
     * }
     * }
     * <p>
     * 否则：IllegalArgumentException[Fielddata is disabled on text fields by default. Set fielddata=true on [account_id] in order to load fielddata in memory by uninverting the inverted index. Note that this can however use significant memory. Alternatively use a keyword field instead.];
     */
    public List<Map<String, Object>> search(String index, String type, ESQueryCriterionBuilderManager manager) {
        List<Map<String, Object>> result = new ArrayList<>();
        SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch(index).setTypes(type);
        //排序
        if (StringUtils.isNotEmpty(manager.getAsc()))
            searchRequestBuilder.addSort(manager.getAsc(), SortOrder.ASC);
        if (StringUtils.isNotEmpty(manager.getDesc()))
            searchRequestBuilder.addSort(manager.getDesc(), SortOrder.DESC);
        //设置查询体
        searchRequestBuilder.setQuery(manager.build());
        //返回条目数
        int size = manager.getSize();
        if (size < 0) {
            size = 0;
        }
        if (size > MAX) {
            size = MAX;
        }
        //返回条目数
        searchRequestBuilder.setSize(size);

        searchRequestBuilder.setFrom(manager.getFrom() < 0 ? 0 : manager.getFrom());

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHists = hits.getHits();
        for (SearchHit sh : searchHists) {
            result.add(sh.getSourceAsMap());
        }
        return result;
    }

    /**
     * @param [index->索引名, type->类型, manager->查询最终构建者, groupBy->根据什么字段分组]
     * @return java.util.Map<java.lang.Object, java.lang.Object>（k:统计字段分组，v:共有多少）
     * @describe 统计查询条数
     * 统计字段需要开启fielddata-->举例：
     * PUT app_account1/_mapping
     * {
     * "properties": {
     * "content": {
     * "type":     "text",
     * "fielddata": true
     * }
     * }
     * }
     * 否则：IllegalArgumentException[Fielddata is disabled on text fields by default. Set fielddata=true on [content] in order to load fielddata in memory by uninverting the inverted index. Note that this can however use significant memory. Alternatively use a keyword field instead.];
     */
    public Map<Object, Object> statSearch(String index, String type, ESQueryCriterionBuilderManager manager, String groupBy) {
        Map<Object, Object> map = new HashedMap();
        SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch(index).setTypes(type);
        //排序
        if (StringUtils.isNotEmpty(manager.getAsc()))
            searchRequestBuilder.addSort(manager.getAsc(), SortOrder.ASC);
        if (StringUtils.isNotEmpty(manager.getDesc()))
            searchRequestBuilder.addSort(manager.getDesc(), SortOrder.DESC);
        //设置查询体
        if (null != manager) {
            searchRequestBuilder.setQuery(manager.build());
        } else {
            searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        }
        int size = manager.getSize();
        if (size < 0) {
            size = 0;
        }
        if (size > MAX) {
            size = MAX;
        }
        //返回条目数
        searchRequestBuilder.setSize(size);

        searchRequestBuilder.setFrom(manager.getFrom() < 0 ? 0 : manager.getFrom());
        searchRequestBuilder.setFrom(manager.getFrom() < 0 ? 0 : manager.getFrom());

        SearchResponse sr = searchRequestBuilder.addAggregation(
                //别名count
                AggregationBuilders.terms("count").field(groupBy)
        ).get();

        Terms stateAgg = sr.getAggregations().get("count");
        Iterator<? extends Terms.Bucket> iterator = stateAgg.getBuckets().iterator();

        while (iterator.hasNext()) {
            Terms.Bucket gradeBucket = iterator.next();
            map.put(gradeBucket.getKey(), gradeBucket.getDocCount());
        }

        return map;
    }

    /** 
    * @param [index->索引名, type->类型, manager->查询最终构建者, esagg->聚合条件构建, args-分组名]
    * @return java.util.Map<java.lang.Object,java.lang.Object> 
    * @describe 自定义聚合计算
    */
    public Map<Object, Object> statSearch(String index, String type, ESQueryCriterionBuilderManager manager, ESAggregationCriterionBuilder esagg,String args) {
        Map<Object, Object> map = new HashedMap();
        SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch(index).setTypes(type);
        //排序
        if (StringUtils.isNotEmpty(manager.getAsc()))
            searchRequestBuilder.addSort(manager.getAsc(), SortOrder.ASC);
        if (StringUtils.isNotEmpty(manager.getDesc()))
            searchRequestBuilder.addSort(manager.getDesc(), SortOrder.DESC);
        //设置查询体
        if (null != manager) {
            searchRequestBuilder.setQuery(manager.build());
        } else {
            searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        }
        int size = manager.getSize();
        if (size < 0) {
            size = 0;
        }
        if (size > MAX) {
            size = MAX;
        }
        //返回条目数
        searchRequestBuilder.setSize(size);

        searchRequestBuilder.setFrom(manager.getFrom() < 0 ? 0 : manager.getFrom());

        SearchResponse sr = searchRequestBuilder.addAggregation(
                esagg.builder()
        ).execute().actionGet();
        Terms stateAgg = sr.getAggregations().get(args);
        Iterator<? extends Terms.Bucket> iterator = stateAgg.getBuckets().iterator();

        while (iterator.hasNext()) {
            Terms.Bucket gradeBucket = iterator.next();
            Aggregation aggregation = gradeBucket.getAggregations().get("agg");
            JSONObject jsonObject = JSONObject.parseObject(aggregation.toString());
            JSONObject value = JSONObject.parseObject(jsonObject.get("agg").toString());
            map.put(gradeBucket.getKey(), value.get("value"));
        }
        return map;
    }
}