package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().executor();
    }


    ConsumerDemoWithThreads(){}

    private void executor(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());
        String topic = "first-topic";
        String bootStrapServer = "127.0.0.1:9092"; //broker address
        String groupId = "consumer-set-12";

        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //create consumer runnable
        Runnable myConsumerRunnable = new ConsumerRunnable(topic,bootStrapServer , groupId , latch);

        //start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();


        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( ()->{
            logger.info("caught shutdown hook");
            ((ConsumerRunnable)myConsumerRunnable).shutDown();

            try {
                latch.await();
            }catch (Exception e){

            }finally {
                logger.info("Application exited !");
            }
        }));


        try{
            latch.await(); //wait untill application is over
        }catch (Exception e){
            logger.info("Application is interrupted");
        }finally {
            logger.info("Application is closing");
        }

    }


    public class ConsumerRunnable implements Runnable{

        CountDownLatch latch;
        private  KafkaConsumer<String,String>consumer;
        private Logger logger;



        ConsumerRunnable(String topic , String bootStrapServer , String groupId , CountDownLatch latch ){
           this.latch = latch;
           this.logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

            //step-1 : create properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootStrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG , groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest"); //we can also have "latest" and "none"

            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Collections.singleton(topic));

        }

        @Override
        public void run(){

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                    for (ConsumerRecord<String, String> r : records) {
                        logger.info("key : " + r.key());
                        logger.info("value : " + r.value());
                        logger.info("partition : " + r.partition());
                        logger.info("offset : " + r.offset());
                    }
                }
            }catch (WakeupException e){
                logger.info("reveived shutdown signal!!");
            }finally {
                consumer.close();

                //tells the main code that we are done with the consumer
                latch.countDown();

            }

        }

        public void shutDown(){
            //the wakeup() is used to interrupt consumer.poll()
            //it will throw WakeUpException
            consumer.wakeup();
        }
    }
}
