package com.jusfoun.es01;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.Client;

import java.util.Map;

/**
 * Created by lisiyu on 16/8/11.
 */
public class TestGet {
    public static void main(String[] args) {
        // on startup
        Client client = MyTransportClient.client;

//        GetResponse getResponse = client.prepareGet("twitter", "tweet", "AVZ5JgyLTP8mxY2wt6l6").get();
//        GetResponse getResponse = client.prepareGet("index", "type", "1").get();
//        GetResponse getResponse = client.prepareGet("twitter", "tweet", "1")
//                .setOperationThreaded(false)
//                .get();

//        Map<String,Object> map = getResponse.getSourceAsMap();
//        for(Map.Entry<String, Object> entry : map.entrySet()){
//            System.out.println(entry.getKey()+"--->"+entry.getValue());
//        }



       MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
           .add("twitter", "tweet", "1")
           .add("twitter", "tweet", "2", "3", "4")
           .add("index", "type", "1")
           .get();

        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                System.out.println("json="+json);
            }
        }









        // on shutdown
        client.close();
    }
}
