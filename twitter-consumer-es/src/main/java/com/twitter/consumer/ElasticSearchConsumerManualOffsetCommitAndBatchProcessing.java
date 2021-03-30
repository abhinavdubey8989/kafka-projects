package com.twitter.consumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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

@SuppressWarnings("deprecation")
public class ElasticSearchConsumerManualOffsetCommitAndBatchProcessing {

   static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumerManualOffsetCommitAndBatchProcessing.class.getName());

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createESClient();
        KafkaConsumer<String,String>consumer = createConsumer("twitter-topic");

        String elasticIndex = "twitter-es-index";
        String indexType = "tweet"; //this can be any string , just required to pass into constructor

        while(true){
            ConsumerRecords<String,String> records =  consumer.poll(Duration.ofSeconds(2));

            Integer count = records.count();
            logger.info("received : " + count + " records");

            //batch processing using ES client
            BulkRequest bulkRequest = new BulkRequest();

            //insert data into ES
            for(ConsumerRecord<String,String> r : records){
                String jsonToInsert = r.value();

                try{
                    //inserting doc with uniq tewwt id will make the processing IDEMPOTENT
                    String idOfDocument = getTweetId(r.value());
                    IndexRequest indexRequest= new IndexRequest(elasticIndex,indexType,idOfDocument ).source(jsonToInsert , XContentType.JSON);
                    bulkRequest.add(indexRequest);
                }catch (Exception e){
                    logger.info("Skipping bad tweet");
                }

            }

            //access ES only when bulk data is ready
            if(count>0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Commiting offsets");
                consumer.commitSync();
                logger.info("Offset committed");
            }
        }

        //close ES client gracefully
        //client.close();

    }





    static KafkaConsumer<String,String> createConsumer(String topic){
        String bootStrapServer = "127.0.0.1:9092"; //broker address
        String groupId = "consumer-set-14";

        //step-1 : create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG , groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest"); //earliest : read from beginning of topic

        //MANUAL OFFEST COMMIT PROPERTIEs
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG , "false");

        //this is just for demo purposes , to fetch 5 records only
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG , "5");


        //step-2 : create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;

    }



    static RestHighLevelClient createESClient(){
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200, "http"));

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }




    static String getTweetId(String jsonTweet){
        //using gson library
        JsonParser jsonParser = new JsonParser();
        String docId = jsonParser.parse(jsonTweet).getAsJsonObject().get("id_str").getAsString();
        return docId;
    }


}
