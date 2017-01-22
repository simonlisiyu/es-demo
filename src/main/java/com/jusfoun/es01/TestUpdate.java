package com.jusfoun.es01;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by lisiyu on 16/8/12.
 */
public class TestUpdate {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        // on startup
        Client client = MyTransportClient.client;

//        UpdateRequest updateRequest = new UpdateRequest();
//        updateRequest.index("twitter");
//        updateRequest.type("tweet");
//        updateRequest.id("1");
//        updateRequest.doc(jsonBuilder()
//                .startObject()
//                .field("gender", "male")
//                .endObject());
//        client.update(updateRequest).get();

//        UpdateRequest updateRequest = new UpdateRequest("twitter", "tweet", "1")
//                .script(new Script("ctx._source.gender = \"male\""));
//        client.update(updateRequest).get();

//        client.prepareUpdate("twitter", "tweet", "1")
//                .setScript(new Script("ctx._source.gender = \"female\""  , ScriptService.ScriptType.INLINE, null, null))
//                .get();

//        client.prepareUpdate("twitter", "tweet", "1")
//                .setDoc(jsonBuilder()
//                        .startObject()
//                        .field("gender", "female")
//                        .endObject())
//                .get();

//        UpdateRequest updateRequest = new UpdateRequest("twitter", "tweet", "1")
//                .doc(jsonBuilder()
//                        .startObject()
//                        .field("gender", "female")
//                        .endObject());
//        client.update(updateRequest).get();






        // upsert
        IndexRequest indexRequest = new IndexRequest("index", "type", "1")
                .source(jsonBuilder()
                        .startObject()
                        .field("name", "Joe Smith")
                        .field("gender", "male")
                        .endObject());
        UpdateRequest updateRequest = new UpdateRequest("index", "type", "1")
                .doc(jsonBuilder()
                        .startObject()
                        .field("gender", "male")
                        .endObject())
                .upsert(indexRequest);
        client.update(updateRequest).get();

        // on shutdown
        client.close();
    }

}
