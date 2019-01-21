import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

/**
 * Created by D-DU72KE on 7/05/2018.
 */
public class SampleProducer {
    Producer<String,String> sampleProducer;
    private static Scanner in;
    public static String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    public static String KAFKA_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static String KAFKA_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static String KAFKA_TOPIC = "KAFKAMAPBLTEST";

    //Main method
    public static void main(String[] args){
        SampleProducer kafkaSampleProducer= new SampleProducer();
        System.out.println("Producer Started .....");
        in = new Scanner(System.in);
        String userInputMessage = in.nextLine();
        while(!userInputMessage.equals("exit")) {
            kafkaSampleProducer.sendMessage(KAFKA_TOPIC,userInputMessage);
            userInputMessage = in.nextLine();
        }
        in.close();
        kafkaSampleProducer.closeProducer();
        System.out.println("Producer Closed .....");
    }

    //Producer Configuration inside the default constructor.
    public SampleProducer(){
        Properties producerConfigProps=new Properties();
        producerConfigProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_BOOTSTRAP_SERVERS);
        producerConfigProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,KAFKA_KEY_SERIALIZER);
        producerConfigProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,KAFKA_VALUE_SERIALIZER);
        sampleProducer=new KafkaProducer<String, String>(producerConfigProps);
    }

    //Send Message
    public void sendMessage(String topicName, String message){
        ProducerRecord<String,String> record= new ProducerRecord<String, String>(topicName,message);
        sampleProducer.send(record);
    }

    //Close Producer
    public void closeProducer(){
        sampleProducer.close();
    }

}
