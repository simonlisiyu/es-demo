package com.jusfoun.es01;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.ExecutionException;

/**
 * Created by lisiyu on 16/9/20.
 */
public class TestAdmin {
    public static void main(String[] args) {
        // on startup
        Client client = MyTransportClient.client;

        GetIndexResponse response = client.admin().indices().prepareGetIndex().execute().actionGet();


        System.out.println(response.getIndices().length);

        String[] indices = response.getIndices();
        for(String indice : indices){
            System.out.println("indice="+indice);
            GetSettingsResponse response1 = client.admin().indices()
                    .prepareGetSettings(indice).get();
            for (ObjectObjectCursor<String, Settings> cursor : response1.getIndexToSettings()) {
                String index = cursor.key;
                Settings settings = cursor.value;
                Integer shards = settings.getAsInt("index.number_of_shards", null);
                Integer replicas = settings.getAsInt("index.number_of_replicas", null);
                System.out.println("index="+index);
                System.out.println("shards="+shards);
                System.out.println("replicas="+replicas);

                GetMappingsResponse res = null;
                try {
                    res = client.admin().indices().getMappings(new GetMappingsRequest().indices(indice)).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                ImmutableOpenMap<String, MappingMetaData> mapping  = res.mappings().get(indice);
                for (ObjectObjectCursor<String, MappingMetaData> c : mapping) {
                    System.out.println("type = "+c.key);
                    System.out.println("columns = "+c.value.source());
                }
            }
        }


        // on shutdown
        client.close();
    }
}
