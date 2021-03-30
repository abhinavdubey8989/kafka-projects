package com.twitter.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    TwitterProducer(){}
    public static void main(String[] args) throws Exception {
        new TwitterProducer().run();
    }

    void run() throws Exception{
        //step-1 : create twitter client
        //Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
        //this will contain the msgs
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);
        Client client = this.createTwitterClient(msgQueue);

        client.connect();

        //step-2 : create kafka producer
        KafkaProducer<String,String>producer = this.createKafkaProducer();

        //OPTIONAL : shutdown hook , works when we click on exit button
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Stopping twitter producer");
            client.stop();
            producer.close();

        }));



        //step-3 : loop and send tweets to kafka
        while (!client.isDone()) {

            String msg=null;
            try {
                msg = msgQueue.poll(2,TimeUnit.SECONDS);
            }catch (InterruptedException e){
                e.printStackTrace();
                client.stop();
            }

            if(msg!=null){
              logger.info(msg);
              logger.info("**************************************");

              ProducerRecord<String,String> record = new ProducerRecord<String,String>("twitter-topic" , null , msg);
              producer.send(record,(recordMetadata, e) -> {
                  if(e!=null){
                    logger.error("something bad happened : " + e.getMessage());
                  }
              });

            }

        }


    }


        Client createTwitterClient(BlockingQueue<String> msgQueue) throws Exception {

            //Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
            Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
            StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

            // Optional: set up some followings and track terms
            List<String> terms = Lists.newArrayList("bitcoin" , "COVID19" , "colors" , "ipl" , "holi" , "Happy Holi" , "india" , "covid" , "corona");
            hosebirdEndpoint.trackTerms(terms);


            //load properties file
            FileInputStream fis = new FileInputStream("/home/abhinav/Documents/spring/kafka-basics/src/main/resources/config.properties");
            Properties prop = new Properties();
            prop.load(fis);

           Authentication hosebirdAuth = new OAuth1(prop.getProperty("cosumerKey") , prop.getProperty("consumerSecret") , prop.getProperty("token") , prop.getProperty("tokenSecret"));



            //now, finally create client
            ClientBuilder builder = new ClientBuilder()
                    .name("Hosebird-Client-01")
                    .hosts(hosebirdHosts)
                    .authentication(hosebirdAuth)
                    .endpoint(hosebirdEndpoint)
                    .processor(new StringDelimitedProcessor(msgQueue));

            Client hosebirdClient = builder.build();
            return hosebirdClient;

    }



    KafkaProducer<String,String> createKafkaProducer(){
        //step-1 : create producer properties
        String bootStrapServer = "127.0.0.1:9092"; //broker address
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , bootStrapServer );
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());

        //CREATING SAFE PRODUCER
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG , "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG , "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG , String.valueOf(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION , "5");

        //ENABLING COMPRESSION and SMART BATCHING
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG , "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG , "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG , String.valueOf(32*1024));







        //step-2 : create producer
        KafkaProducer<String,String>producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
