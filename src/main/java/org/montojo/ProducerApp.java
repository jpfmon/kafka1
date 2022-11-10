package org.montojo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class ProducerApp
{
    public static void main( String[] args )
    {

        //creating logger
        final Logger logger = LoggerFactory.getLogger(ProducerApp.class);
        //create Producer properties

//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers","localhost:9092");
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.serializer", StringSerializer.class.getName());
//        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
//        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        at least one producer
//        properties.setProperty(ProducerConfig.ACKS_CONFIG,"1");

        //create Producer
        KafkaProducer<String, String> producer = getProducer();
        Scanner scanner = new Scanner(System.in);
        while (true){
            System.out.println("Enter message");
            String input = scanner.nextLine();

            if (input.equals("exit")){
                System.out.println("Closing app..... bye bye");
                scanner.close();
                break;
            } else {
                String[] inputArray = input.split("%");
                String text = inputArray[0].trim();
                String key = inputArray[1] == null ? "" : inputArray[1].trim();
                producer.send(produceRecord(text, key), new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null){
                            //successfully sent
                            logger.info("Message sent successfully: " +
                                    recordMetadata.topic() + " Partition "
                                    + recordMetadata.partition() + " Offset "
                                    + recordMetadata.offset());
                        } else {
                            //exception happened
                            logger.warn("Message not sent succesfully");
                        }
                    }
                });
            }
        }
        producer.flush();
        producer.close();
        //create data
//        ProducerRecord<String, String> record = produceRecord("from Java App");
        //send data
//        producer.send(record);
    }

    private static KafkaProducer<String, String> getProducer(){

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        at least one producer
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"1");
        //create Producer
        return new KafkaProducer<String, String>(properties);
    }

    private static ProducerRecord<String, String> produceRecord(String text, String key){
        return new ProducerRecord<String, String>("firsttopic", key, text);
    }
}
