package com.jusfoun.es04;

import com.jusfoun.es01.MyTransportClient;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.*;

/**
 * Created by lisiyu on 2017/1/24.
 */
public class EsVmIdCount {
    static Client client = MyTransportClient.client;

    public static void main(String[] args) {

        int gzServerCount = 0;
        int bjServerCount = 0;
        int sqServerCount = 0;
        int hkServerCount = 0;
        int gzYesterdayCount = 0;
        int bjYesterdayCount = 0;
        int sqYesterdayCount = 0;
        int hkYesterdayCount = 0;
        int gzErrorCount = 0;
        int bjErrorCount = 0;
        int sqErrorCount = 0;
        int hkErrorCount = 0;
        String[] attackTypeCNs = {"TCP连接数", "连通性", "CPU Load", "SWAP使用率", "内存使用率", "磁盘繁忙",
                "磁盘使用率", "CPU利用率", "网络流入包数", "网络流出包数", "网络流出速率", "网络流入速率"};
        Map<String, Long> bjErrMap = null;
        Map<String, Long> gzErrMap = null;
        Map<String, Long> sqErrMap = null;
        Map<String, Long> hkErrMap = null;

        Date dayBeforeSevenDays = new Date(System.currentTimeMillis() - 7 * 24 * 60 * 60 * 1000);
        Date dayBeforeYesterday = new Date(System.currentTimeMillis() - 3 * 24 * 60 * 60 * 1000);
        Date yesterday = new Date(System.currentTimeMillis() - 3 * 24 * 60 * 60 * 1000);
        Date today = new Date(System.currentTimeMillis() - 0 * 24 * 60 * 60 * 1000);

        /**
         * 主机数量统计
         */
        QueryBuilder bj_err_week = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("isAlarm", "true"))
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.fuzzyQuery("idc", "gz01"))
                        .should(QueryBuilders.fuzzyQuery("idc", "gz100")));
//                .must(QueryBuilders.rangeQuery("datetime").to(today).from(yesterday));

        /**
         * 执行query
         */
//        SearchRequestBuilder builder = client
//                .prepareSearch("mjdos")
//                .setQuery(bj_err_week)
//                .setFrom(0).setSize(60).setExplain(true);
//        SearchResponse response = builder.execute().actionGet();

        SearchRequestBuilder builder = client
                .prepareSearch("mjdos")
                .setQuery(bj_err_week)
//                .addAggregation(AggregationBuilders.terms("attackType").field("attackType").size(0))
                .setFrom(0).setSize(1).setExplain(true);
        SearchResponse response  = builder.execute().actionGet();

        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> map = hit.getSource();
            System.out.println(map.get("env"));
            System.out.println(map.get("idc"));
            System.out.println(map.get("vm_id"));
        }
//        System.out.println(response.getHits().hits());
        System.out.println(response.getHits().getTotalHits());

//        StringTerms st = (StringTerms) response.getAggregations().getAsMap().getOrDefault("host",null);
//        StringTerms st = (StringTerms) response.getAggregations().getAsMap().getOrDefault("attackType",null);
//        Iterator<Terms.Bucket> gradeBucketIt = st.getBuckets().iterator();
//        int i = 0;
//        while(gradeBucketIt.hasNext())
//        {
//            Terms.Bucket gradeBucket = gradeBucketIt.next();
//            System.out.println(gradeBucket.getKey() + "有" + gradeBucket.getDocCount() +"个。");
//            i++;
//        }
//        System.out.println("key="+i);

    }
}
