package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class.getName());
        String bootStrapServer = "127.0.0.1:9092"; //broker address
        String groupId = "consumer-set-9";
        String topic = "first-topic";


        //step-1 : create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG , groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest"); //we can also have "latest" and "none"

        //earliest : read from beginning of topic
        //latest : read new msgs
        //none : throws an error if no offsets are saved


        //step-2 : create consumer
        KafkaConsumer<String,String>consumer = new KafkaConsumer<String, String>(properties);


        //ASSIGN AND SEEK ARE MOSTLY USED TO FETCH A SPECIFIC MSG AND REPLAY DATA

        //1. ASSIGN
        TopicPartition partitionToReadFrom = new TopicPartition(topic,0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //2. SEEK
        int offsetToReadFrom = 10;
        consumer.seek(partitionToReadFrom,offsetToReadFrom);


        //lets say we will read only 3 msgs
        int msgLeft=3;
        boolean exitLoop = false;


        //step-4 : poll for new data
        while(true){
            ConsumerRecords<String,String>records =  consumer.poll(Duration.ofSeconds(2));

            for(ConsumerRecord<String,String> r : records){
                logger.info("key : " + r.key());
                logger.info("value : " + r.value());
                logger.info("partition : " + r.partition());
                logger.info("offset : " + r.offset());

                msgLeft--;
                if(msgLeft==0){
                    exitLoop=true;
                    break;
                }

            }

            if(exitLoop){
                logger.info("DONE PROCESSING");
            }

        }


    }
}
