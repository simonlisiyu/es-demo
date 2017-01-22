package com.jusfoun.es01;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.jusfoun.es01.MyTransportClient.client;

/**
 * Created by lisiyu on 16/9/9.
 */
public class TestBulkProcessor {

    public static void main(String[] args) {
        // on startup
        Client client = MyTransportClient.client;
        Set<String> set = new HashSet<>();
        set.add("aaa");
        set.add("field");

        BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
//                        try {
//                            createMapping1("test1",  "data", set);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {

                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                         }
                })
                .setBulkActions(1)
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();

        Map<String, Object> json = new HashMap<String, Object>();
        json.put("field", "中文软件系统");
        json.put("aaa", "测试大学考试");

        Map<String, Object> json1 = new HashMap<String, Object>();
        json1.put("field1", "1中文软件系统");
        json1.put("aaa1", "1测试大学考试");
        bulkProcessor.add(new UpdateRequest("test1", "data", "222").doc(json1).docAsUpsert(true));
//        bulkProcessor.add(new UpdateRequest("test1", "data", "222").doc(json1).upsert(new IndexRequest("test1", "data", "222").source(json)));
//        bulkProcessor.add(new UpdateRequest("test1", "data", "222").doc(json));

        json = new HashMap<String, Object>();
        json.put("field1", "1中文软件系统");
        json.put("log", "A06-R12-I53-47 sudo:  neutron : TTY=unknown ; PWD=/ ; USER=root ; \n" +
                "                                   COMMAND=/usr/bin/neutron-rootwrap /etc/neutron/rootwrap.conf ovs-vsctl --timeout=10 list-br||A06-\n" +
                "                                   R12-I53-47 sudo:  neutron : TTY=unknown ; PWD=/ ; USER=root ; COMMAND=/usr/bin/neutron-rootwrap /\n" +
                "                                   etc/neutron/rootwrap.conf ovs-ofctl --timeout=5 dump-flows br-int table=22||");
//        bulkProcessor.add(new IndexRequest("test1", "data", "555").source(json));
        bulkProcessor.add(new UpdateRequest("test1", "data", "555").doc(json));

    }

    /**
     * 创建mapping(feid("indexAnalyzer","ik")该字段分词IK索引 ；feid("searchAnalyzer","ik")该字段分词ik查询；具体分词插件请看IK分词插件说明)
     * @param index 索引名称；
     * @param mappingType 索引类型
     * @throws Exception
     */
    public static void createMapping(String index,String mappingType)throws Exception{
//        System.out.println(client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists());
        // 判断index是否存在,不存在则创建索引,并启用ik分词器
        if(client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists()){
            System.out.println("index: '"+index+"' is exist!");
        } else {
            new XContentFactory();
            XContentBuilder builder=XContentFactory.jsonBuilder()
                    .startObject()//注意不要加index和type
                    .startObject("properties")
                    .startObject("id").field("type", "integer").field("store", "yes").endObject()
                    .startObject("field").field("type", "string").field("store", "yes").field("analyzer", "ik").endObject()
//                .startObject("edate33").field("type", "date").field("store", "yes").endObject()
                    .endObject()
                    .endObject();

            client.admin().indices().prepareCreate(index).addMapping(mappingType, builder).get();
//        PutMappingRequest mapping = Requests.putMappingRequest(index).type(mappingType).source(builder);
//        client.admin().indices().putMapping(mapping).actionGet();
        }
    }

    /**
     * 创建mapping(feid("indexAnalyzer","ik")该字段分词IK索引 ；feid("searchAnalyzer","ik")该字段分词ik查询；具体分词插件请看IK分词插件说明)
     * @param index 索引名称；
     * @param mappingType 索引类型
     * @param fieldSet 列集合
     * @throws Exception
     */
    public static void createMapping1(String index,String mappingType,Set<String> fieldSet)throws Exception{
        // 判断index是否存在,不存在则创建索引,并启用ik分词器
        if(client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists()){
            System.out.println("index: '"+index+"' is exist!");
        } else {
            new XContentFactory();
            XContentBuilder builder=XContentFactory.jsonBuilder()
                    .startObject()//注意不要加index和type
                    .startObject("properties")
                    .startObject("id").field("type", "string").field("store", "yes").endObject();
            for(String field : fieldSet){
                builder.startObject(field).field("type", "string").field("store", "yes").field("analyzer", "ik").endObject();
            }
            builder.endObject().endObject();

            client.admin().indices().prepareCreate(index).addMapping(mappingType, builder).get();
        }
    }




}
