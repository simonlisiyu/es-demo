package com.jusfoun.es03;

import com.jusfoun.commons.IdGen;
import com.jusfoun.es01.MyTransportClient;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by lisiyu on 2017/1/22.
 */
public class EsImport2 {
    public static void main(String[] args) {
        String index = "mjdos";
        String type = "log";
        String fileName = "es";
        if (args.length == 3) {
            index = args[0];
            type = args[1];
            fileName = args[2];
            System.out.println("index="+index);
            System.out.println("type="+type);
            System.out.println("fileName="+fileName);
        }

        Client client = MyTransportClient.client;
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
                .setBulkActions(5000)
                .setBulkSize(new ByteSizeValue(1024, ByteSizeUnit.GB))
                .setFlushInterval(TimeValue.timeValueSeconds(60))
                .setConcurrentRequests(3)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(6000), 3))
                .build();

        String id = IdGen.uuid();
        try {
            //读取刚才导出的ES数据
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            String json = null;
            int count = 0;

            while ((json = br.readLine()) != null) {
                UpdateRequest updateRequest = new UpdateRequest(index, type, id+(++count)).doc(json);
                bulkProcessor.add(updateRequest.docAsUpsert(true));
            }

            System.out.println("import finished.");
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
