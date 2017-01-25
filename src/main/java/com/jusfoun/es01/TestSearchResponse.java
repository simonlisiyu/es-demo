package com.jusfoun.es01;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;

/**
 * Created by lisiyu on 16/8/11.
 */
public class TestSearchResponse {
    public static void main(String[] args) throws UnknownHostException {
        // on startup
        Client client = MyTransportClient.client;
//        try{
//            SearchResponse response = client.prepareSearch().setQuery(QueryBuilders.matchQuery("url", "twitter")).setSize(5).execute().actionGet();//bunch of urls indexed
//            String output = response.toString();
//            System.out.println(output);
//        }catch(Exception e){
//            e.printStackTrace();
//        }

//        SearchResponse response = client.prepareSearch("hbase", "bank")
//                .setTypes("test", "test1")
//                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .setQuery(QueryBuilders.termQuery("PARAM_NAME", "a"))                 // Query
//                .setPostFilter(QueryBuilders.rangeQuery("PARAM_ID").from(20).to(20))     // Filter
//                .setFrom(0).setSize(60).setExplain(true)
//                .execute()
//                .actionGet();

//        SearchResponse response = client.prepareSearch("hbase", "bank")
//                .setTypes("test", "test2")
//                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .setQuery(QueryBuilders.fuzzyQuery("1", "222"))                 // Query
//                .setFrom(0).setSize(60).setExplain(true)
//                .execute()
//                .actionGet();

//        SearchResponse response = client.prepareSearch("hbase", "bank")
//                .setTypes("test", "test2")
//                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .setQuery(QueryBuilders
//
////                        .prefixQuery("PARAM_NAME","a"))
//                        .queryStringQuery("asdf"))                 // Query
//                .setFrom(0).setSize(60).setExplain(true)
//                .execute()
//                .actionGet();

//        SearchResponse response = client.prepareSearch("test", "bank")
//                .setTypes("account", "data")
//                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .setQuery(QueryBuilders.fuzzyQuery("1", "中文"))                 // Query
//                .addHighlightedField("1")
//                .setFrom(0).setSize(60).setExplain(true)
//                .execute()
//                .actionGet();

//        MatchQueryBuilder qb = QueryBuilders.matchQuery("_all", "a");
//        SearchRequestBuilder srb =client.prepareSearch("hbase");
//        srb.addHighlightedField("name");
//        srb.setHighlighterPreTags("<b>");
//        srb.setHighlighterPostTags("</b>");
//        srb.setQuery(qb);

//        SearchResponse response = client.prepareSearch("hbase", "bank")
//                .setTypes("test", "test2")
//                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .setQuery(QueryBuilders.queryStringQuery("A"))
//                .addHighlightedField("PARAM_NAME")
//                .setHighlighterPreTags("<strong>")
//                .setHighlighterPostTags("</strong>")
//                .setExplain(true)
//                .setFrom(0).setSize(60).setExplain(true)
//                .execute()
//                .actionGet();

        String query = "{\"query_string\": {\"query\": \"98.88\"}}";
        SearchRequestBuilder builder = client
                .prepareSearch("t2")
//                .setQuery(query)
                .setQuery(QueryBuilders.termQuery("kw", "disk.io"))
                .addAggregation(AggregationBuilders.terms("kw").field("kw"))
                .addHighlightedField("*")
                .setHighlighterRequireFieldMatch(false)
                .setFrom(0).setSize(60).setExplain(true);
        SearchResponse response  = builder.execute().actionGet();

//        System.out.println(response.toString());
        System.out.println(response.getHits().getTotalHits());
//        System.out.println(response.getAggregations().getAsMap().getOrDefault("kw",null));
        StringTerms st = (StringTerms) response.getAggregations().getAsMap().getOrDefault("kw",null);
        Iterator<Terms.Bucket> gradeBucketIt = st.getBuckets().iterator();
        int i = 0;
        while(gradeBucketIt.hasNext())
        {
            Terms.Bucket gradeBucket = gradeBucketIt.next();
            System.out.println(gradeBucket.getKey() + "有" + gradeBucket.getDocCount() +"个。");
            i++;
        }
        System.out.println("key="+i);
//        System.out.println(response.getHits().getAt(0).getSource());
//        System.out.println(response.getHits().getAt(1).getSource());
//        System.out.println(response.getHits().getAt(0).getHighlightFields());
        client.close();
    }

}
