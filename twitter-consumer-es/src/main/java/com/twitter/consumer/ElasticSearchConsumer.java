package com.twitter.consumer;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticSearchConsumer {

   static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createESClient();

        String doc = "{ \"foo\" : \"bar\"}";

        IndexRequest indexRequest= new IndexRequest("twitter-es-index" ).source(doc , XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest , RequestOptions.DEFAULT);
        String id =  indexResponse.getId();

        logger.info(id + " sent to ES");

        client.close();

    }




    public static RestHighLevelClient createESClient(){
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200, "http"));

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}
