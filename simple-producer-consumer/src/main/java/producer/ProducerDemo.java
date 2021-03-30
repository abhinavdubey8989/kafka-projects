package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        //step-1 : create producer properties
        String bootStrapServer = "127.0.0.1:9092"; //broker address
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , bootStrapServer );
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());

        //step-2 : create producer
        KafkaProducer<String,String>producer = new KafkaProducer<String, String>(properties);

        //step-3 : create producer record
        ProducerRecord<String,String>record = new ProducerRecord<String,String>("first-topic" , "hello world");



        //step-4 : send data/producer-record using producer (this is async operation)
        producer.send(record);

        producer.flush();//flush data
        //producer.close();//flush data and close producer
    }

}
