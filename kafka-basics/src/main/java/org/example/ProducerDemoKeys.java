package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

        public static void main(String[] args) {
            log.info("I am a producer !!");

            //create Producer properties
            Properties properties = new Properties();

            //connect to localhost
           // properties.setProperty("bootstrap.servers","127.0.0.1:9092");

            // connect to Conduktor cluster
            properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
            properties.setProperty("security.protocol","SASL_SSL");
            properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"12i59O86Kh2MdkTX68ugQD\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIxMmk1OU84NktoMk1ka1RYNjh1Z1FEIiwib3JnYW5pemF0aW9uSWQiOjc0NDMwLCJ1c2VySWQiOjg2NTgyLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIxYjkzMDUyMi03OTZkLTQ3ZTMtYTQ3ZC1lOWQxZjNlOWZmYzQifX0.bIzCqFTKXklp4784pV48Hv8y8Tv4xPLGAV8dZVkSpoc\";");
            properties.setProperty("sasl.mechanism","PLAIN");

            //set producer properties
            properties.setProperty("key.serializer", StringSerializer.class.getName());
            properties.setProperty("value.serializer", StringSerializer.class.getName());
            //create the producer
            KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
            for(int j=0;j<2;j++)
            {
                for(int i=0;i<10;i++) {
                    String topic = "demo_java";
                    String key = "id_" + i;
                    String value = "hello world " + i + "in " + j;
                    //create a producer record
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, value, key);

                    //send data
                    producer.send(producerRecord, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            //executes every time a record is sent successfully or an exception is thrown
                            if (e == null) {
                                // record was successfully sent
                                log.info("Key : " + key + " | Partition : " + metadata.partition());
                            } else {
                                // an exception has occurred
                                log.error("Exception has occurred : " + e);
                            }
                        }
                    });
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
            }

            //tell the producer to send all the date and block until done -- synchronous
            producer.flush();

            // flush and close the producer
            producer.close();

        }
}
