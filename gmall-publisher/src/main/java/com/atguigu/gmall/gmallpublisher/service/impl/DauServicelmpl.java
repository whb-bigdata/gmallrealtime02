package com.atguigu.gmall.gmallpublisher.service.impl;

import com.atguigu.gmall.gmallpublisher.service.DauService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DauServicelmpl implements DauService {

    @Autowired
    JestClient jestClint;

    @Override
    public Long getFauTotal(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        String indexName = "gmall_dau_info" + date;
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
        SearchResult searchResult = null;
        try {
            searchResult = jestClint.execute(search);
        } catch (IOException e) {

            e.printStackTrace();
            throw new RuntimeException("es查询异常");
        }
        Long total = searchResult.getTotal();
        return total;
    }

    @Override
    public Map<String, Long> getDauHour(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String indexName = "gmall_dau_info" + date;

        final TermsBuilder aggBuilder = AggregationBuilders.terms("grouby_hr").field("hr").size(24);
        searchSourceBuilder.aggregation(aggBuilder);


        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
        SearchResult searchResult = null;
        try {
            searchResult = jestClint.execute(search);
        } catch (IOException e) {

            e.printStackTrace();
            throw new RuntimeException("es查询异常");
        }
        Map<String, Long> rsMap = new HashMap<>();

        TermsAggregation termsAggregation = searchResult.getAggregations().getTermsAggregation("grouby_hr");
        if (termsAggregation != null) {
            List<TermsAggregation.Entry> buckets = termsAggregation.getBuckets();

            for (TermsAggregation.Entry bucket : buckets) {
                rsMap.put(bucket.getKey(), bucket.getCount());
            }

        }
        return rsMap;
    }
}
























