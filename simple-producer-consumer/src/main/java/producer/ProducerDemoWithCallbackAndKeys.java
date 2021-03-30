package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithCallbackAndKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //creating a logger for class named ProducerDemoWithCallback
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbackAndKeys.class);

        //step-1 : create producer properties
        String bootStrapServer = "127.0.0.1:9092"; //broker address
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , bootStrapServer );
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());

        //step-2 : create producer
        KafkaProducer<String,String>producer = new KafkaProducer<String, String>(properties);


        //step-3 : create and send data/producer-record using producer (this is async operation)
        //here we have added callback
        for(int i=0;i<10;i++) {
            String topic = "first-topic";
            String key = "id_"+String.valueOf(i);
            String value = "hello world " + String.valueOf(i);

            logger.info("key : " + key);

            //partition-0 : ids(1,3,6)
            //partition-1 : ids(0,8)
            //partition-2 : ids(2,4,5,7,9)

            ProducerRecord<String,String>record = new ProducerRecord<String,String>(topic,key,value);
            producer.send(record, (recordMetadata, e) -> {

                if (e == null) {
                    logger.info("******* Received Metadata *******");
                    logger.info("Topic : " + recordMetadata.topic());
                    logger.info("Partition : " + recordMetadata.partition());
                    logger.info("Offset : " + recordMetadata.offset());
                } else {
                    logger.error("unable to send data : " + e.getMessage());
                }

            }).get(); //forcing send() to be synchronous , don't do it in prod
        }


        producer.flush();


    }

}
