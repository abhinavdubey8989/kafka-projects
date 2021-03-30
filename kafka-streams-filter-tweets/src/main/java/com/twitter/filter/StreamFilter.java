package com.twitter.filter;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamFilter {

    public static void main(String[] args) {
        //step-1 : create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG , "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG , "demo-stream"); //similar to consumer grup , but for stream application
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG , Serdes.StringSerde.class.getName()); //key will be string serialized
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG , Serdes.StringSerde.class.getName());


        //step-2 : create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //topic to read from
        KStream<String,String> inputTopic=  streamsBuilder.stream("twitter-topic");
        KStream<String,String> filteredTopic =  inputTopic.filter((k,v)->{
            //filter users having >10k followers : here v is json tweet
           return getFollowers(v) > 10000;
        });

        filteredTopic.to("important-tweets");

        //step-3 : build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build() , properties);


        //step-4 : start stream java application
        kafkaStreams.start();
    }



    static int getFollowers(String jsonTweet){
        //using gson library
        try{
            JsonParser jsonParser = new JsonParser();
            Integer followers = jsonParser.parse(jsonTweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
            return (followers==null || followers <= 0)?0:followers;
        }catch (Exception e){
            return 0;
        }
    }


}
