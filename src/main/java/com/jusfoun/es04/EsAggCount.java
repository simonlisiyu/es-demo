package com.jusfoun.es04;

import com.jusfoun.es01.MyTransportClient;
import org.apache.lucene.store.StoreUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;

/**
 * Created by lisiyu on 2017/1/24.
 */
public class EsAggCount {
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

        Date dayBeforeSevenDays = new Date(System.currentTimeMillis()-7*24*60*60*1000);
        Date dayBeforeYesterday = new Date(System.currentTimeMillis()-3*24*60*60*1000);
        Date yesterday = new Date(System.currentTimeMillis()-3*24*60*60*1000);
        Date today = new Date(System.currentTimeMillis()-0*24*60*60*1000);

        /**
         * 主机数量统计
         */
        /*// 广州
        QueryBuilder gz_all_today = QueryBuilders.boolQuery()
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.fuzzyQuery("idc","gz01")))
                .must(QueryBuilders.rangeQuery("datetime").to(today).from(yesterday));
        QueryBuilder gz_all_yesterday = QueryBuilders.boolQuery()
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.fuzzyQuery("idc", "gz01")))
                .must(QueryBuilders.rangeQuery("datetime").to(yesterday).from(dayBeforeYesterday));
        QueryBuilder gz_err_week = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("isAlarm","true"))
                .must(QueryBuilders.fuzzyQuery("idc","gz01"))
                .must(QueryBuilders.rangeQuery("datetime").to(today).from(yesterday));

        // 华北
        QueryBuilder bj_all_today = QueryBuilders.boolQuery()
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.fuzzyQuery("idc", "bj01"))
                        .should(QueryBuilders.fuzzyQuery("idc", "bj100")))
                .must(QueryBuilders.rangeQuery("datetime").to(today).from(yesterday));
        QueryBuilder bj_all_yesterday = QueryBuilders.boolQuery()
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.fuzzyQuery("idc", "bj01"))
                        .should(QueryBuilders.fuzzyQuery("idc", "bj100")))
                .must(QueryBuilders.rangeQuery("datetime").to(yesterday).from(dayBeforeYesterday));
        QueryBuilder bj_err_week = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("isAlarm","true"))
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.fuzzyQuery("idc", "bj01"))
                        .should(QueryBuilders.fuzzyQuery("idc", "bj100")))
                .must(QueryBuilders.rangeQuery("datetime").to(today).from(yesterday));

        // 宿迁
        QueryBuilder sq_all_today = QueryBuilders.boolQuery()
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.fuzzyQuery("idc", "sq01")))
                .must(QueryBuilders.rangeQuery("datetime").to(today).from(yesterday));
        QueryBuilder sq_all_yesterday = QueryBuilders.boolQuery()
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.fuzzyQuery("idc", "sq01")))
                .must(QueryBuilders.rangeQuery("datetime").to(yesterday).from(dayBeforeYesterday));
        QueryBuilder sq_err_week = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("isAlarm","true"))
                .must(QueryBuilders.fuzzyQuery("idc","sq01"))
                .must(QueryBuilders.rangeQuery("datetime").to(today).from(yesterday));

        // 香港
        QueryBuilder hk_all_today = QueryBuilders.boolQuery()
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.fuzzyQuery("idc", "hk01")))
                .must(QueryBuilders.rangeQuery("datetime").to(today).from(yesterday));
        QueryBuilder hk_all_yesterday = QueryBuilders.boolQuery()
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.fuzzyQuery("idc", "hk01")))
                .must(QueryBuilders.rangeQuery("datetime").to(yesterday).from(dayBeforeYesterday));
        QueryBuilder hk_err_week = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("isAlarm","true"))
                .must(QueryBuilders.fuzzyQuery("idc","hk01"))
                .must(QueryBuilders.rangeQuery("datetime").to(today).from(yesterday));*/


        /**
         * 执行query
         */
        /*SearchRequestBuilder builder = client
                .prepareSearch("mjdos")
                .setQuery(gz_all_today)
                .addAggregation(AggregationBuilders.terms("host").field("host").size(0))
                .setFrom(0).setSize(60).setExplain(true);
        SearchResponse response  = builder.execute().actionGet();

        QueryBuilder sq_err_net = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("isAlarm","true"))
                .must(QueryBuilders.termQuery("attackType","网络流入包数"))
                .must(QueryBuilders.fuzzyQuery("idc","sq01"))
                .must(QueryBuilders.rangeQuery("datetime").to(today).from(yesterday));
        builder = client
                .prepareSearch("mjdos")
                .setQuery(sq_err_net)
//                .addAggregation(AggregationBuilders.terms("attackType").field("attackType").size(0))
                .setFrom(0).setSize(1).setExplain(true);
        response  = builder.execute().actionGet();

        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> map = hit.getSource();
            System.out.println(map.get("timestamp"));
            System.out.println(map.get("env"));
            System.out.println(map.get("idc"));
            System.out.println(map.get("app"));
            System.out.println(map.get("service"));
            System.out.println(map.get("host"));
            System.out.println(map.get("attackPurpose"));
        }
//        System.out.println(response.getHits().hits());
        System.out.println(response.getHits().getTotalHits());*/

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

        /**
         * 测试封装方法
         */
        /*
        System.out.println(getESErrorCount("mjdos", "host", "gz01", today, yesterday));

        gzErrMap = getESErrorMap("mjdos", "attackType", "gz01", today, yesterday);
        for (String attackType : attackTypeCNs){
            System.out.println(gzErrMap.getOrDefault(attackType, 0L)==0L);
        }*/
        Set<String[]> set = getESErrorMsgs("mjdos", "网络流入包数", "gz01", today, yesterday, 1);
        for(String[] strs : set){
            System.out.println(strs[1]);
        }




        /*bjServerCount = getESCount("mjdos", "host", "bj01", "bj100", today, yesterday);
        gzServerCount = getESCount("mjdos", "host", "gz01", today, yesterday);
        sqServerCount = getESCount("mjdos", "host", "sq01", today, yesterday);
        hkServerCount = getESCount("mjdos", "host", "hk01", today, yesterday);
        bjYesterdayCount = getESCount("mjdos", "host", "bj01", "bj100", yesterday, dayBeforeYesterday);
        gzYesterdayCount = getESCount("mjdos", "host", "gz01", yesterday, dayBeforeYesterday);
        sqYesterdayCount = getESCount("mjdos", "host", "sq01", yesterday, dayBeforeYesterday);
        hkYesterdayCount = getESCount("mjdos", "host", "hk01", yesterday, dayBeforeYesterday);
        bjErrorCount = getESErrorCount("mjdos", "host", "bj01", "bj100", today, yesterday);
        gzErrorCount = getESErrorCount("mjdos", "host", "gz01", today, yesterday);
        sqErrorCount = getESErrorCount("mjdos", "host", "sq01", today, yesterday);
        hkErrorCount = getESErrorCount("mjdos", "host", "hk01", today, yesterday);

        if(bjErrorCount!=0) bjErrMap = getESErrorMap("mjdos", "attackType", "bj01", "bj100", today, yesterday);
        if(gzErrorCount!=0) gzErrMap = getESErrorMap("mjdos", "attackType", "gz01", today, yesterday);
        if(sqErrorCount!=0) sqErrMap = getESErrorMap("mjdos", "attackType", "sq01", today, yesterday);
        if(hkErrorCount!=0) hkErrMap = getESErrorMap("mjdos", "attackType", "hk01", today, yesterday);

        String content = "<html><body><h5>主机数量</h5>\n" +
                "   <table border=\"1\" cellspacing=\"0\" bordercolor=\"#000000\" width = \"80%\" style=\"border-collapse:collapse;\">" +
                "<thead><tr><th>节点</th><th>当前主机数量</th><th>变更数量</th><th>异常主机数量</th></tr></thead><tbody>\n" +
                "<!--for -->\n" +
                "<tr><td>华北</td><td>"+bjServerCount+"</td><td>"+(bjServerCount-bjYesterdayCount)+"</td><td>"+bjErrorCount+"</td></tr>\n" +
                "<tr><td>华南</td><td>"+gzServerCount+"</td><td>"+(gzServerCount-gzYesterdayCount)+"</td><td>"+gzErrorCount+"</td></tr>\n" +
                "<tr><td>华东</td><td>"+sqServerCount+"</td><td>"+(sqServerCount-sqYesterdayCount)+"</td><td>"+sqErrorCount+"</td></tr>\n" +
                "<tr><td>香港</td><td>"+hkServerCount+"</td><td>"+(hkServerCount-hkYesterdayCount)+"</td><td>"+hkErrorCount+"</td></tr>\n" +
                "<!--for end -->\n";


        if(bjErrorCount!=0 && bjErrMap!=null){
            content += "</tbody></table><h5>华北异常类型统计</h5>\n" +
                    "<table border=\"1\" cellspacing=\"0\" bordercolor=\"#000000\" width = \"40%\" style=\"border-collapse:collapse;\">\n" +
                    "<thead><tr><th>异常类型</th><th>异常统计数量</th></tr></thead><tbody>\n" +
                    "<!--for -->\n";
            for(String attackType : attackTypeCNs){
                if(bjErrMap.getOrDefault(attackType, 0L)!=0L){
                    content +="<tr><td>"+attackType+"</td><td bgcolor=yellow>"+bjErrMap.get(attackType)+"</td></tr>";
                }
            }
        }

        if(gzErrorCount!=0 && gzErrMap!=null){
            content += "<!--for end -->" +
                    "</tbody></table><h5>华南异常类型统计</h5>\n" +
                    "<table border=\"1\" cellspacing=\"0\" bordercolor=\"#000000\" width = \"40%\" style=\"border-collapse:collapse;\">\n" +
                    "<thead><tr><th>异常类型</th><th>异常统计数量</th></tr></thead><tbody>\n" +
                    "<!--for -->\n";
            for(String attackType : attackTypeCNs){
                if(gzErrMap.getOrDefault(attackType, 0L)!=0L){
                    System.out.println(attackType);
                    content +="<tr><td>"+attackType+"</td><td bgcolor=yellow>"+gzErrMap.get(attackType)+"</td></tr>";
                }
            }
        }

        if(sqErrorCount!=0 && sqErrMap!=null){
            content += "<!--for end -->\n" +
                    "</tbody></table><h5>华东异常类型统计</h5>\n" +
                    "<table border=\"1\" cellspacing=\"0\" bordercolor=\"#000000\" width = \"40%\" style=\"border-collapse:collapse;\">\n" +
                    "<thead><tr><th>异常类型</th><th>异常统计数量</th></tr></thead><tbody>\n" +
                    "<!--for -->\n";
            for(String attackType : attackTypeCNs){
                if(sqErrMap.getOrDefault(attackType, 0L)!=0L){
                    content +="<tr><td>"+attackType+"</td><td bgcolor=yellow>"+sqErrMap.get(attackType)+"</td></tr>";
                }
            }
        }

        if(hkErrorCount!=0 && hkErrMap!=null){
            content += "<!--for end -->\n" +
                    "</tbody></table><h5>香港异常类型统计</h5>\n" +
                    "<table border=\"1\" cellspacing=\"0\" bordercolor=\"#000000\" width = \"40%\" style=\"border-collapse:collapse;\">\n" +
                    "<thead><tr><th>异常类型</th><th>异常统计数量</th></tr></thead><tbody>\n" +
                    "<!--for -->\n";
            for(String attackType : attackTypeCNs){
                if(hkErrMap.getOrDefault(attackType, 0L)!=0L){
                    content +="<tr><td>"+attackType+"</td><td bgcolor=yellow>"+hkErrMap.get(attackType)+"</td></tr>";
                }
            }
        }

        content += "<!--for end --></tbody></table></body></html>";

        HttpUtils.doPost("http://mail.iaas.jcloud.com",
                "tolist=lisiyu3@jd.com&cclist=&subject=告警邮件&content="+content);*/

        client.close();
    }

    private static int getESCount(String index, String fieldForAgg, String idc, Date to, Date from){
        QueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.fuzzyQuery("idc",idc)))
                .must(QueryBuilders.rangeQuery("datetime").to(to).from(from));

        return getCountFromQuery(index, query, fieldForAgg);
    }
    private static int getESCount(String index, String fieldForAgg, String idc1, String idc2, Date to, Date from){
        QueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.fuzzyQuery("idc", idc1))
                        .should(QueryBuilders.fuzzyQuery("idc", idc2)))
                .must(QueryBuilders.rangeQuery("datetime").to(to).from(from));

        return getCountFromQuery(index, query, fieldForAgg);
    }

    private static int getESErrorCount(String index, String fieldForAgg, String idc, Date to, Date from){
        QueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("isAlarm","true"))
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.fuzzyQuery("idc",idc)))
                .must(QueryBuilders.rangeQuery("datetime").to(to).from(from));

        return getCountFromQuery(index, query, fieldForAgg);
    }
    private static int getESErrorCount(String index, String fieldForAgg, String idc1, String idc2, Date to, Date from){
        QueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("isAlarm","true"))
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.fuzzyQuery("idc", idc1))
                        .should(QueryBuilders.fuzzyQuery("idc", idc2)))
                .must(QueryBuilders.rangeQuery("datetime").to(to).from(from));

        return getCountFromQuery(index, query, fieldForAgg);
    }

    private static Map<String, Long> getESErrorMap(String index, String fieldForAgg, String idc, Date to, Date from){
        QueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("isAlarm","true"))
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.fuzzyQuery("idc",idc)))
                .must(QueryBuilders.rangeQuery("datetime").to(to).from(from));

        return getMapFromQuery(index, query, fieldForAgg);
    }

    private static Map<String, Long> getESErrorMap(String index, String fieldForAgg, String idc1, String idc2, Date to, Date from){
        QueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("isAlarm","true"))
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.fuzzyQuery("idc", idc1))
                        .should(QueryBuilders.fuzzyQuery("idc", idc2)))
                .must(QueryBuilders.rangeQuery("datetime").to(to).from(from));

        return getMapFromQuery(index, query, fieldForAgg);
    }

    private static int getCountFromQuery(String index, QueryBuilder query, String fieldForAgg){
        SearchRequestBuilder builder = client
                .prepareSearch(index)
                .setQuery(query)
                .addAggregation(AggregationBuilders.terms(fieldForAgg).field(fieldForAgg).size(0))
                .setFrom(0).setSize(60).setExplain(true);
        SearchResponse response  = builder.execute().actionGet();
        StringTerms st = (StringTerms) response.getAggregations().getAsMap().getOrDefault(fieldForAgg,null);
        if(st == null){
            return 0;
        }
        Iterator<Terms.Bucket> gradeBucketIt = st.getBuckets().iterator();
        int i = 0;
        while(gradeBucketIt.hasNext())
        {
            gradeBucketIt.next();
            i++;
        }
        return i;
    }

    private static Map<String,Long> getMapFromQuery(String index, QueryBuilder query, String fieldForAgg) {
        Map<String,Long> fieldMap = new HashMap<>();
        SearchRequestBuilder builder = client
                .prepareSearch(index)
                .setQuery(query)
                .addAggregation(AggregationBuilders.terms(fieldForAgg).field(fieldForAgg).size(0))
                .setFrom(0).setSize(60).setExplain(true);
        SearchResponse response  = builder.execute().actionGet();
        StringTerms st = (StringTerms) response.getAggregations().getAsMap().getOrDefault(fieldForAgg,null);
        if(st == null){
            return null;
        }
        Iterator<Terms.Bucket> gradeBucketIt = st.getBuckets().iterator();
        while(gradeBucketIt.hasNext())
        {
            Terms.Bucket gradeBucket = gradeBucketIt.next();
            fieldMap.put(gradeBucket.getKey().toString(), gradeBucket.getDocCount());
        }
        return fieldMap;
    }

    private static Set<String[]> getESErrorMsgs(String index, String attackType, String idc, Date to, Date from, int size) {
        QueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("isAlarm","true"))
                .must(QueryBuilders.termQuery("attackType",attackType))
                .must(QueryBuilders.fuzzyQuery("idc",idc))
                .must(QueryBuilders.rangeQuery("datetime").to(to).from(from));
        return getMsgSetFromQuery(index, query, size);
    }

    private static Set<String[]> getMsgSetFromQuery(String index, QueryBuilder query, int size) {
        Set<String[]> resultSet = new HashSet<>();
        SearchRequestBuilder builder = client
                .prepareSearch(index)
                .setQuery(query)
                .setFrom(0).setSize(size).setExplain(true);
        SearchResponse response  = builder.execute().actionGet();

        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> map = hit.getSource();
            String[] hitResult = new String[7];
            hitResult[0] = map.get("timestamp").toString();
            hitResult[1] = map.get("env").toString();
            hitResult[2] = map.get("idc").toString();
            hitResult[3] = map.get("app").toString();
            hitResult[4] = map.get("service").toString();
            hitResult[5] = map.get("host").toString();
            hitResult[6] = map.get("attackPurpose").toString();
            resultSet.add(hitResult);
        }

        return resultSet;
    }
}
