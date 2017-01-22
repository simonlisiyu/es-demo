package com.jusfoun.es01;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

/**
 * Created by lisiyu on 16/8/11.
 */
public class MyTransportClient {

    public static Settings settings;
    public static Client client;

    static {
        settings = Settings.settingsBuilder()
                .put("cluster.name", "iaas.log").build();
        try {
            client = TransportClient.builder().settings(settings).build()
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.4.206"), 9300))
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.146.129"), 9300));
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.27.37.12"), 9300));
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("172.27.36.60"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";

        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("user", "kimchy")
                .field("postDate", new Date())
                .field("message", "trying out Elasticsearch")
                .endObject();

        // on startup

        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", "iaas.log").build();
//        Client client = null;
        client = TransportClient.builder().settings(settings).build()
//                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.5.131"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.146.129"), 9300));

        // index
        IndexResponse response = client.prepareIndex("twitter", "tweet", "1")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject()
                )
                .get();

        response = client.prepareIndex("twitter", "tweet")
                .setSource(json)
                .get();


        // Index name
        String _index = response.getIndex();
        // Type name
        String _type = response.getType();
        // Document ID (generated or not)
        String _id = response.getId();
        // Version (if it's the first time you index this document, you will get: 1)
        long _version = response.getVersion();
        // isCreated() is true if the document is a new one, false if it has been updated
        boolean created = response.isCreated();



        // update
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("index");
        updateRequest.type("type");
        updateRequest.id("1");
        updateRequest.doc(jsonBuilder()
                .startObject()
                .field("gender", "male")
                .endObject());
        client.update(updateRequest).get();



        client.prepareUpdate("ttl", "doc", "1")
                .setScript(new Script("ctx._source.gender = \"male\""  , ScriptService.ScriptType.INLINE, null, null))
                .get();

        client.prepareUpdate("ttl", "doc", "1")
                .setDoc(jsonBuilder()
                        .startObject()
                        .field("gender", "male")
                        .endObject())
                .get();

        updateRequest = new UpdateRequest("ttl", "doc", "1")
                .script(new Script("ctx._source.gender = \"male\""));
        client.update(updateRequest).get();



        //upsert
        IndexRequest indexRequest = new IndexRequest("index", "type", "1")
                .source(jsonBuilder()
                        .startObject()
                        .field("name", "Joe Smith")
                        .field("gender", "male")
                        .endObject());
        UpdateRequest upsertRequest = new UpdateRequest("index", "type", "1")
                .doc(jsonBuilder()
                        .startObject()
                        .field("gender", "male")
                        .endObject())
                .upsert(indexRequest);
        client.update(upsertRequest).get();


        // get
        GetResponse getResponse = client.prepareGet("twitter", "tweet", "1").get();
        getResponse = client.prepareGet("twitter", "tweet", "1")
                .setOperationThreaded(false)
                .get();

        // delete
        DeleteResponse deleteResponse = client.prepareDelete("twitter", "tweet", "1").get();
        deleteResponse = client.prepareDelete("twitter", "tweet", "1")
//                .setOperationThreaded(false)
                .get();

        // on shutdown
        client.close();
    }


}
