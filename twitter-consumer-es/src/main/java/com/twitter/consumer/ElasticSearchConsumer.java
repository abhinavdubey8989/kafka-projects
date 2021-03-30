package com.twitter.consumer;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

   static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createESClient();
        KafkaConsumer<String,String>consumer = createConsumer("twitter-topic");

        //TESTING ES INSERTION
        // String doc = "{ \"foo\" : \"bar\"}";
        // IndexRequest indexRequest= new IndexRequest("twitter-es-index" ).source(doc , XContentType.JSON);
        // IndexResponse indexResponse = client.index(indexRequest , RequestOptions.DEFAULT);
        // String id =  indexResponse.getId();
        // logger.info(id + " sent to ES");


        while(true){
            ConsumerRecords<String,String> records =  consumer.poll(Duration.ofSeconds(2));

            //insert data into ES
            for(ConsumerRecord<String,String> r : records){
                String jsonToInsert = r.value();
                IndexRequest indexRequest= new IndexRequest("twitter-es-index" ).source(jsonToInsert , XContentType.JSON);
                IndexResponse indexResponse = client.index(indexRequest , RequestOptions.DEFAULT);
                String id =  indexResponse.getId();
                logger.info(id + " sent to ES");

//                try {
//                    Thread.sleep(2000); //intoducing small delay
//                }catch (Exception e){
//
//                }



            }
        }

        //close ES client gracefully
        //client.close();

    }





    public static KafkaConsumer<String,String> createConsumer(String topic){
        String bootStrapServer = "127.0.0.1:9092"; //broker address
        String groupId = "consumer-set-14";

        //step-1 : create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG , groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest"); //earliest : read from beginning of topic

        //step-2 : create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;

    }



    public static RestHighLevelClient createESClient(){
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200, "http"));

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }





}
