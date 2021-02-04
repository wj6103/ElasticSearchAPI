package com.silkrode.ai.poa.repository;


import com.silkrode.ai.poa.DTO.*;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.json.JSONObject;
import org.springframework.stereotype.Repository;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;

import java.sql.Timestamp;
import java.util.*;

@Repository
public class ESRepository {
    private RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("10.205.48.52", 9200)));

    public Object category(String index, Timestamp startTime, Timestamp endTime) throws Exception {
        Request scriptRequest = new Request("POST", "_scripts/title_search");
        SearchTemplateRequest request = new SearchTemplateRequest();
        request.setRequest(new SearchRequest(index));
        request.setScriptType(ScriptType.INLINE);
        request.setScript(
                " {\n" +
                        "          \"aggs\": {\n" +
                        "            \"category\": {\n" +
                        "              \"terms\": {\n" +
                        "                \"field\": \"category.keyword\",\n" +
                        "                \"order\": {\n" +
                        "                  \"_count\": \"desc\"\n" +
                        "                },\n" +
                        "                \"size\": 12\n" +
                        "              }\n" +
                        "            }\n" +
                        "          },\n" +
                        "          \"size\": 0,\n" +
                        "          \"_source\": {\n" +
                        "            \"excludes\": []\n" +
                        "          },\n" +
                        "          \"stored_fields\": [\n" +
                        "            \"*\"\n" +
                        "          ],\n" +
                        "          \"script_fields\": {},\n" +
                        "          \"docvalue_fields\": [\n" +
                        "            {\n" +
                        "              \"field\": \"date\",\n" +
                        "              \"format\": \"date_time\"\n" +
                        "            }\n" +
                        "          ],\n" +
                        "          \"query\": {\n" +
                        "            \"bool\": {\n" +
                        "              \"must\": [],\n" +
                        "              \"filter\": [],\n" +
                        "              \"should\": [],\n" +
                        "              \"must_not\": []\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }"
        );
        Map<String, Object> params = new HashMap<>();
        params.put("value", "elasticsearch");
        params.put("size", 10);
        request.setScriptParams(params);

        SearchTemplateResponse response = client.searchTemplate(request, RequestOptions.DEFAULT);
        Terms terms = response.getResponse().getAggregations().get("category");
        List<Bucket> buckets = (List<Bucket>) terms.getBuckets();
        Category category = new Category();
        ArrayList<Map> categoryList = new ArrayList<>();

        for (Bucket b : buckets) {
            Map<String, String> m = new HashMap<>();
            m.put("key", b.getKey().toString());
            m.put("doc_count", String.valueOf(b.getDocCount()));
            categoryList.add(m);
        }
        category.setCategory(categoryList);
        return category;
    }

    public Object issues(String index, int size, String filter, Timestamp startTime, Timestamp endTime) throws Exception {
        Request scriptRequest = new Request("POST", "_scripts/title_search");
        SearchTemplateRequest request = new SearchTemplateRequest();
        request.setRequest(new SearchRequest(index));
        filter = "\"filter\": [" + filter + "],";
        if (size == 0)
            size = 100;
        request.setScriptType(ScriptType.INLINE);
        request.setScript(
                "{\n" +
                        "          \"aggs\": {\n" +
                        "            \"issues\": {\n" +
                        "              \"terms\": {\n" +
                        "                \"field\": \"issue.keyword\",\n" +
                        "                \"order\": {\n" +
                        "                  \"_count\": \"desc\"\n" +
                        "                },\n" +
                        "                \"size\": " + size + "\n" +
                        "              }\n" +
                        "            }\n" +
                        "          },\n" +
                        "          \"size\": 0,\n" +
                        "          \"_source\": {\n" +
                        "            \"excludes\": []\n" +
                        "          },\n" +
                        "          \"stored_fields\": [\n" +
                        "            \"*\"\n" +
                        "          ],\n" +
                        "          \"script_fields\": {},\n" +
                        "          \"docvalue_fields\": [\n" +
                        "            {\n" +
                        "              \"field\": \"date\",\n" +
                        "              \"format\": \"date_time\"\n" +
                        "            }\n" +
                        "          ],\n" +
                        "          \"query\": {\n" +
                        "            \"bool\": {\n" +
                        "              \"must\": [],\n" + filter +
                        "              \"should\": [],\n" +
                        "              \"must_not\": []\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n");
        Map<String, Object> params = new HashMap<>();
        params.put("value", "elasticsearch");
        params.put("size", 10);
        request.setScriptParams(params);

        SearchTemplateResponse response = client.searchTemplate(request, RequestOptions.DEFAULT);
        Terms terms = response.getResponse().getAggregations().get("issues");
        List<Bucket> buckets = (List<Bucket>) terms.getBuckets();
        Issues issues = new Issues();
        ArrayList<Map> issuesList = new ArrayList<>();

        for (Bucket b : buckets) {
            Map<String, String> m = new HashMap<>();
            m.put("key", b.getKey().toString());
            m.put("doc_count", String.valueOf(b.getDocCount()));
            issuesList.add(m);
        }
        issues.setKeywords(issuesList);
        return issues;
    }

    public Object keywords(String index, int size, String filter, Timestamp startTime, Timestamp endTime) throws Exception {

        Request scriptRequest = new Request("POST", "_scripts/title_search");
        SearchTemplateRequest request = new SearchTemplateRequest();
        request.setRequest(new SearchRequest(index));
        filter = "\"filter\": [" + filter + "],";
        if (size == 0)
            size = 50;
        request.setScriptType(ScriptType.INLINE);
        request.setScript(
                " {\n" +
                        "          \"aggs\": {\n" +
                        "            \"keywords\": {\n" +
                        "              \"terms\": {\n" +
                        "                \"field\": \"keywords_event.keyword\",\n" +
                        "                \"order\": {\n" +
                        "                  \"_count\": \"desc\"\n" +
                        "                },\n" +
                        "                \"size\": " + size + "\n" +
                        "              }\n" +
                        "            }\n" +
                        "          },\n" +
                        "          \"size\": 0,\n" +
                        "          \"_source\": {\n" +
                        "            \"excludes\": []\n" +
                        "          },\n" +
                        "          \"stored_fields\": [\n" +
                        "            \"*\"\n" +
                        "          ],\n" +
                        "          \"script_fields\": {},\n" +
                        "          \"docvalue_fields\": [\n" +
                        "            {\n" +
                        "              \"field\": \"date\",\n" +
                        "              \"format\": \"date_time\"\n" +
                        "            }\n" +
                        "          ],\n" +
                        "          \"query\": {\n" +
                        "            \"bool\": {\n" +
                        "              \"must\": [],\n" + filter +
                        "              \"should\": [],\n" +
                        "              \"must_not\": []\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }");
        Map<String, Object> params = new HashMap<>();
        params.put("value", "elasticsearch");
        params.put("size", 10);
        request.setScriptParams(params);

        SearchTemplateResponse response = client.searchTemplate(request, RequestOptions.DEFAULT);
        Terms terms = response.getResponse().getAggregations().get("keywords");
        List<Bucket> buckets = (List<Bucket>) terms.getBuckets();
        Keywords keywords = new Keywords();
        ArrayList<Map> keywordsList = new ArrayList<>();

        for (Bucket b : buckets) {
            Map<String, String> m = new HashMap<>();
            m.put("key", b.getKey().toString());
            m.put("doc_count", String.valueOf(b.getDocCount()));
            keywordsList.add(m);
        }
        keywords.setKeywords(keywordsList);
        return keywords;
    }

    public Object issueDetail(String index, int size, String filter, Timestamp startTime, Timestamp endTime) throws Exception {
        Request scriptRequest = new Request("POST", "_scripts/title_search");
        SearchTemplateRequest request = new SearchTemplateRequest();
        request.setRequest(new SearchRequest(index));
        filter = "\"filter\": [" + filter + "],";
        if (size == 0)
            size = 100;
        request.setScriptType(ScriptType.INLINE);
        request.setScript(
                " {\n" +
                        "          \"aggs\": {\n" +
                        "            \"category\": {\n" +
                        "              \"terms\": {\n" +
                        "                \"field\": \"category.keyword\",\n" +
                        "                \"size\": 12\n" +
                        "              },\n" +
                        "              \"aggs\": {\n" +
                        "                \"issue\": {\n" +
                        "                  \"terms\": {\n" +
                        "                    \"field\": \"issue.keyword\",\n" +
                        "                    \"size\": " + size + "\n" +
                        "                  },\n" +
                        "                  \"aggs\": {\n" +
                        "                    \"comment_count\": {\n" +
                        "                      \"sum\": {\n" +
                        "                        \"field\": \"comment_count_event\"\n" +
                        "                      }\n" +
                        "                    },\n" +
                        "                    \"summary_issue\": {\n" +
                        "                      \"terms\": {\n" +
                        "                        \"field\": \"summary_issue.keyword\",\n" +
                        "                        \"size\": " + size + "\n" +
                        "                      }\n" +
                        "                    },\n" +
                        "                    \"sentiment_title\": {\n" +
                        "                      \"avg\": {\n" +
                        "                        \"field\": \"sentiment_title_event\"\n" +
                        "                      }\n" +
                        "                    },\n" +
                        "                    \"most_liked_comment\": {\n" +
                        "                      \"terms\": {\n" +
                        "                        \"field\": \"most_liked_comment_issue.keyword\",\n" +
                        "                        \"missing\": \"__missing__\",\n" +
                        "                        \"size\": " + size + "\n" +
                        "                      }\n" +
                        "                    },\n" +
                        "                    \"event\": {\n" +
                        "                      \"cardinality\": {\n" +
                        "                        \"field\": \"event.keyword\"\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }\n" +
                        "          },\n" +
                        "          \"size\": 0,\n" +
                        "          \"_source\": {\n" +
                        "            \"excludes\": []\n" +
                        "          },\n" +
                        "          \"stored_fields\": [\n" +
                        "            \"*\"\n" +
                        "          ],\n" +
                        "          \"script_fields\": {},\n" +
                        "          \"docvalue_fields\": [\n" +
                        "            {\n" +
                        "              \"field\": \"date\",\n" +
                        "              \"format\": \"date_time\"\n" +
                        "            }\n" +
                        "          ],\n" +
                        "          \"query\": {\n" +
                        "            \"bool\": {\n" +
                        "              \"must\": [],\n" + filter +
                        "              \"should\": [],\n" +
                        "              \"must_not\": []\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n");
        Map<String, Object> params = new HashMap<>();
        params.put("value", "elasticsearch");
        params.put("size", 10);
        request.setScriptParams(params);

        SearchTemplateResponse response = client.searchTemplate(request, RequestOptions.DEFAULT);
        System.out.println(response.getResponse());
        Terms terms = response.getResponse().getAggregations().get("category");
        List<Bucket> buckets = (List<Bucket>) terms.getBuckets();
        Issues_Details issues_details = new Issues_Details();
        ArrayList<Map> issues_detailsList = new ArrayList<>();

        for (Bucket b : buckets) {
            Terms termsIssue = b.getAggregations().get("issue");
            List<Bucket> bucketsIssue = (List<Bucket>) termsIssue.getBuckets();
            for (Bucket bi : bucketsIssue) {
                Map<String, String> m = new HashMap<>();
                m.put("key", b.getKeyAsString());
                m.put("issue", bi.getKeyAsString());

                Sum comment_count = bi.getAggregations().get("comment_count");
                Double comment_count_value = comment_count.getValue();
                m.put("total_comment", comment_count_value.toString());

                Cardinality event = bi.getAggregations().get("event");
                long event_value = event.getValue();
                m.put("total_news", String.valueOf(event_value));

                Terms termsSummary_issue = bi.getAggregations().get("summary_issue");
                List<Bucket> bucketsSummary_issue = (List<Bucket>) termsSummary_issue.getBuckets();
                for (Bucket bsi : bucketsSummary_issue)
                    m.put("summary_issue", String.valueOf(event_value));

                Avg sentiment_title = bi.getAggregations().get("sentiment_title");
                Double sentiment_title_value = sentiment_title.getValue();
                m.put("sentiment_title", sentiment_title_value.toString());

                Terms termsMost_liked_comment = bi.getAggregations().get("most_liked_comment");
                List<Bucket> bucketsMost_liked_comment = (List<Bucket>) termsMost_liked_comment.getBuckets();
                for (Bucket bmlc : bucketsMost_liked_comment) {
                    if (bmlc.getKeyAsString().equals("__missing__"))
                        m.put("most_liked_comment", "None");
                    else
                        m.put("most_liked_comment", bmlc.getKeyAsString());
                }
                issues_detailsList.add(m);
            }


        }
        issues_details.setDetails(issues_detailsList);
        return issues_details;
    }

    public Object newsDetail(String index, List<String> columns, int size, String filter, Timestamp startTime, Timestamp endTime) throws Exception {

        Request scriptRequest = new Request("POST", "_scripts/title_search");
        SearchTemplateRequest request = new SearchTemplateRequest();
        request.setRequest(new SearchRequest(index));
        filter = "\"filter\": [" + filter + "],";
        if (size == 0)
            size = 1000;
        String queryColumns = "";
        if (columns == null)
            queryColumns = "[]";
        else {
            queryColumns = "[";
            for (String s : columns) {
                queryColumns += "\"" + s + "\",";
            }
            queryColumns = queryColumns.substring(0, queryColumns.length() - 1) + "]";
        }
        request.setScriptType(ScriptType.INLINE);
        request.setScript(
                " {\n" +
                        "          \"_source\": " + queryColumns + ",\n" +
                        "          \"size\": " + size + ",\n" +
                        "          \"query\": {\n" +
                        "            \"bool\": {\n" +
                        "              \"must\": [],\n" + filter +
                        "              \"should\": []\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n\n");
        Map<String, Object> params = new HashMap<>();
        params.put("value", "elasticsearch");
        params.put("size", 10);
        request.setScriptParams(params);


        SearchTemplateResponse response = client.searchTemplate(request, RequestOptions.DEFAULT);
        SearchHits newsDetails = response.getResponse().getHits();
        News_Details news_details = new News_Details();
        ArrayList<Map> newsDetailsList = new ArrayList<>();
        for (SearchHit nd : newsDetails) {
            Map m = nd.getSourceAsMap();
            newsDetailsList.add(m);
        }
        news_details.setDetails(newsDetailsList);
        return news_details;
    }

    public Object sentiments(String index, List<String> sentimentsList, String filter, Timestamp startTime, Timestamp endTime) throws Exception {

        Request scriptRequest = new Request("POST", "_scripts/title_search");
        SearchTemplateRequest request = new SearchTemplateRequest();
        request.setRequest(new SearchRequest(index));
        filter = "\"filter\": [" + filter + "],";
        String getSentiments = "";
        if(sentimentsList != null){
            getSentiments += "{";
            for (int i=0;i<sentimentsList.size();i++){
                getSentiments += "\""+sentimentsList.get(i)+"\":{\"avg\":{\"field\":\"sentiment_comments_event."+sentimentsList.get(i)+"\"}},";
            }
            getSentiments  = getSentiments.substring(0,getSentiments.length()-1) + "}";
        }
        else {
            getSentiments = "{\"Angry\":{\"avg\":{\"field\":\"sentiment_comments_event.Angry\"}}," +
                    "\"Complain\":{\"avg\":{\"field\":\"sentiment_comments_event.Complain\"}}," +
                    "\"Confused\":{\"avg\":{\"field\":\"sentiment_comments_event.Confused\"}}," +
                    "\"Disapointment\":{\"avg\":{\"field\":\"sentiment_comments_event.Disapointment\"}}," +
                    "\"Tease/Ironic\":{\"avg\":{\"field\":\"sentiment_comments_event.Tease/Ironic\"}}," +
                    "\"Delight/Like\":{\"avg\":{\"field\":\"sentiment_comments_event.Delight/Like\"}}," +
                    "\"Compliment\":{\"avg\":{\"field\":\"sentiment_comments_event.Compliment\"}}}";
        }
        request.setScriptType(ScriptType.INLINE);
        request.setScript(
                " {\n" +
                        "          \"aggs\": " + getSentiments + ",\n" +
                        "          \"size\": 0,\n" +
                        "          \"_source\": {\n" +
                        "            \"excludes\": []\n" +
                        "          },\n" +
                        "          \"stored_fields\": [\n" +
                        "            \"*\"\n" +
                        "          ],\n" +
                        "          \"script_fields\": {},\n" +
                        "          \"docvalue_fields\": [],\n" +
                        "          \"query\": {\n" +
                        "            \"bool\": {\n" +
                        "              \"must\": [],\n" + filter +
                        "              \"should\": [],\n" +
                        "              \"must_not\": []\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n");
        Map<String, Object> params = new HashMap<>();
        params.put("value", "elasticsearch");
        params.put("size", 10);
        request.setScriptParams(params);


        SearchTemplateResponse response = client.searchTemplate(request, RequestOptions.DEFAULT);
        Sentiments sentiments = new Sentiments();
        ArrayList<Map> semtimentsL = new ArrayList<>();
        Map m = new HashMap();

        if(sentimentsList != null) {
            for(int i=0;i<sentimentsList.size();i++) {
                Avg avg = response.getResponse().getAggregations().get(sentimentsList.get(i));
                m.put(avg.getName(), avg.getValue());
            }
            sentiments.setSentiments(m);
        }
        else{
            List<String> l = new ArrayList<String>();
            l.add("Angry"); l.add("Complain"); l.add("Confused"); l.add("Disapointment"); l.add("Tease/Ironic"); l.add("Delight/Like"); l.add("Compliment");
            for(int i=0;i<l.size();i++) {
                Avg avg = response.getResponse().getAggregations().get(l.get(i));
                m.put(avg.getName(), avg.getValue());
            }

            sentiments.setSentiments(m);
        }
        return sentiments;
    }
}
