package com.jusfoun.es03;

import com.jusfoun.es01.MyTransportClient;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;

import java.io.*;

/**
 * Created by lisiyu on 2017/1/22.
 */
public class EsImport {
    public static void main(String[] args) {
        String index = "mjdos2";
        String type = "log";
        String fileName = "es";
        if (args.length == 2) {
            index = args[0];
            type = args[1];
            fileName = args[2];
        }

        Client client = MyTransportClient.client;

        try {
            //读取刚才导出的ES数据
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            String json = null;
            int count = 0;
            //开启批量插入
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            while ((json = br.readLine()) != null) {
                bulkRequest.add(client.prepareIndex(index, type).setSource(json));
                //每一千条提交一次
                if (count% 1000==0) {
                    bulkRequest.execute().actionGet();
                    System.out.println("提交了：" + count);
                }
                count++;
            }
            bulkRequest.execute().actionGet();
            System.out.println("插入完毕");
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


}
