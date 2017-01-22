package com.jusfoun.es01;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.Client;

/**
 * Created by lisiyu on 16/8/12.
 */
public class TestDelete {
    public static void main(String[] args) {
        // on startup
        Client client = MyTransportClient.client;

//        DeleteResponse response = client.prepareDelete("twitter", "tweet", "1").get();

        DeleteResponse response = client.prepareDelete("twitter", "tweet", "1")
//                .setOperationThreaded(false)
                .get();

        // on shutdown
        client.close();
    }
}
