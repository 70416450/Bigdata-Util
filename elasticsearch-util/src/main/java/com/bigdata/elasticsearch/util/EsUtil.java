package com.bigdata.elasticsearch.util;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

/**
 * ES工具类
 */
@SuppressWarnings("all")
@Slf4j
@Component
public class EsUtil {
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
                try {
                    Settings settings = Settings.builder().put("cluster.name", clusterName).build();
                    client = new PreBuiltTransportClient(settings)
                            .addTransportAddress(new TransportAddress(InetAddress.getByName(hostName), tcpPort));
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
            }
        }
        return client;
    }

    /**
     * @param []
     * @return org.elasticsearch.client.IndicesAdminClient
     * @describe 获取索引管理的IndicesAdminClient
     */
    private static IndicesAdminClient getAdminClient() {
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
        return createIndexResponse.isAcknowledged() ? true : false;
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
        return createIndexResponse.isAcknowledged() ? true : false;
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
        return acknowledgedResponse.isAcknowledged() ? true : false;
    }

    /**
     * @param [indexName->索引名, type->类型, doc->XContentBuilder]
     * @return void
     * @describe 插入文档
     */
    public static void insertDocument(String indexName, String type, XContentBuilder doc) {
        IndexResponse response = getClient().prepareIndex(indexName, type)
                .setSource(doc)
                .get();
        System.out.println(response.status());
    }


    /**
     * @param [indexName->索引名, type->类型, json->Json格式串]
     * @return void
     * @describe 插入文档
     */
    public static void insertDocument(String indexName, String type, String json) {
        IndexResponse response = getClient().prepareIndex(indexName, type)
                .setSource(json, XContentType.JSON)
                .get();
        System.out.println(response.status());
    }

    /**
     * @param [indexName->索引名, type->类型, id->文档id]
     * @return java.lang.String
     * @describe 查询文档
     */
    public static String selectDocument(String indexName, String type, String id) {
        GetResponse response = getClient().prepareGet(indexName, type, id).get();
//        System.out.println("是否存在"+response.isExists());
//        System.out.println("索引名"+response.getIndex());
//        System.out.println("类型"+response.getType());
//        System.out.println("文档id"+response.getId());
//        System.out.println("版本"+response.getVersion());
        return response.getSourceAsString();
    }

    /**
     * @param [indexName->索引名, type->类型, id->文档id]
     * @return void
     * @describe 删除文档
     */
    public static int deleteDocument(String indexName, String type, String id) {
        DeleteResponse response = getClient().prepareDelete(indexName, type, id).get();
//        System.out.println("被删除文档的类型"+response.getType());
//        System.out.println("被删除文档的ID"+response.getId());
//        System.out.println("被删除文档的版本信息"+response.getVersion());
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
    public static int updateDocument(String indexName, String type, String id, XContentBuilder doc) throws ExecutionException, InterruptedException {
        UpdateRequest request = new UpdateRequest();
        request.index(indexName).type(type).id(id).doc(doc);
        UpdateResponse response = getClient().update(request).get();
//        System.out.println("被更新文档的类型"+response.getType());
//        System.out.println("被更新文档的ID"+response.getId());
//        System.out.println("被更新文档的版本信息"+response.getVersion());
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
    public static int upsertDocument(String indexName, String type, String id, XContentBuilder insertDoc, XContentBuilder updateDoc) throws ExecutionException, InterruptedException {
        IndexRequest indexRequest = new IndexRequest(indexName, type, id)
                .source(insertDoc);
        UpdateRequest updateRequest = new UpdateRequest(indexName, type, id)
                .doc(updateDoc).upsert(indexRequest);
        UpdateResponse response = getClient().update(updateRequest).get();
//        System.out.println("被操作文档的类型"+response.getType());
//        System.out.println("被操作文档的ID"+response.getId());
//        System.out.println("被操作文档的版本信息"+response.getVersion());
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

}