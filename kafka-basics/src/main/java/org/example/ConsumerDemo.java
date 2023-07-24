package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

        public static void main(String[] args) {
            log.info("I am a consumer !!");
            String groupID = "my-java-application-test";
            String topic = "demo_java";
            //create Producer properties
            Properties properties = new Properties();

            //connect to localhost
           // properties.setProperty("bootstrap.servers","127.0.0.1:9092");

            // connect to Conduktor cluster
            properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
            properties.setProperty("security.protocol","SASL_SSL");
            properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"12i59O86Kh2MdkTX68ugQD\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIxMmk1OU84NktoMk1ka1RYNjh1Z1FEIiwib3JnYW5pemF0aW9uSWQiOjc0NDMwLCJ1c2VySWQiOjg2NTgyLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIxYjkzMDUyMi03OTZkLTQ3ZTMtYTQ3ZC1lOWQxZjNlOWZmYzQifX0.bIzCqFTKXklp4784pV48Hv8y8Tv4xPLGAV8dZVkSpoc\";");
            properties.setProperty("sasl.mechanism","PLAIN");

            //set consumer properties
            properties.setProperty("key.deserializer", StringDeserializer.class.getName());
            properties.setProperty("value.deserializer", StringDeserializer.class.getName());
            properties.setProperty("group.id",groupID);
            properties.setProperty("auto.offset.reset", "earliest");

            //create the consumer
            KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

            //subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            //poll for data
            while(true){
                log.info("Polling !!");
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record : records){
                    log.info("Key : "+record.key()+" , Value :"+record.value());
                    log.info("Partition : "+record.partition()+" , Offset : "+record.offset());
                }
            }
        }
}
