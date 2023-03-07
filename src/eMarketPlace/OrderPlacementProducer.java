package eMarketPlace;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.Future;

public class OrderPlacementProducer{

    private static Logger logger = LoggerFactory.getLogger(OrderPlacementProducer.class);

    public static void main(String[] args) throws UnknownHostException {

        logger.info("I am a Kafka Producer");
        System.out.println(InetAddress.getLocalHost().getHostName());
        System.out.println(InetAddress.getLocalHost().getCanonicalHostName());

//        String bootstrapServers = "127.0.0.1:9092";
//        String bootstrapServers = "[::1]:9092"; // windows local
        String bootstrapServers = "localhost:9092"; // vm port
//        String bootstrapServers = "localhost:9092";

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //create a producer record
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("test","hello world");

        //send data
        Future<RecordMetadata> future = producer.send(record);
        try {
            RecordMetadata recordMetadata = future.get();
            System.out.println("Produce ok:" + recordMetadata.toString());
        } catch (Throwable t) {
            t.printStackTrace();
        }

        //flush + close
        producer.flush();
        producer.close();
    }
}