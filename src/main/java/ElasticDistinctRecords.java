import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class ElasticDistinctRecords {
	public static void main(String[] args) {
        
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));
 
         
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("coachingclassesidx");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.aggregation(AggregationBuilders.terms("DISTINCT_VALUES").field("instructor.keyword"));
        searchRequest.source(searchSourceBuilder);
        Map<String, Object> map=null;
          
        try {
            SearchResponse searchResponse = null;
            searchResponse =client.search(searchRequest, RequestOptions.DEFAULT);
            if (searchResponse.getHits().getTotalHits().value > 0) {
                SearchHit[] searchHit = searchResponse.getHits().getHits();
                for (SearchHit hit : searchHit) {
                    map = hit.getSourceAsMap();
                      System.out.println("map:"+Arrays.toString(map.entrySet().toArray()));
                         
                     
                }
            }
            Aggregations aggregations = searchResponse.getAggregations();
           List<String> list=new ArrayList<String>();
           Terms aggTerms= aggregations.get("DISTINCT_VALUES");
            List<? extends Terms.Bucket> buckets = aggTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                list.add(bucket.getKeyAsString());
            }
            System.out.println("DISTINCT list values:"+list.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
         
    }
}
