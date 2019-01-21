import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by D-DU72KE on 7/05/2018.
 */
public class SampleConsumer {

    public static String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    public static String KAFKA_KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static String KAFKA_VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static String KAFKA_TOPIC = "KAFKAMAPBLTEST";
    public static String KAFKA_CONSUMER_GROUP = "MAPBLTEST";
    private static Scanner in;

    //Main method
    public static void main(String[] args){
        ConsumerThread consumerRunnable= new ConsumerThread(KAFKA_TOPIC);
        consumerRunnable.start();
        System.out.println("Consumer Started .....");
        in = new Scanner(System.in);
        String userInput = "";
        while (!userInput.equals("exit")) {
            userInput = in.next();
        }
        in.close();
        consumerRunnable.getConsumer().wakeup();
        System.out.println("Consumer Closed .....");
        try {
            consumerRunnable.join();
        } catch (InterruptedException e) {
            System.out.println("Exception caught ....."+ e);
        }

    }

    //Inner Class
    private static class ConsumerThread extends Thread {
        private Consumer<String,String> sampleConsumer;
        private String topicName;

        public ConsumerThread(String topicName){
            this.topicName=topicName;
            Properties consumerConfigProps=new Properties();
            consumerConfigProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_BOOTSTRAP_SERVERS);
            consumerConfigProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,KAFKA_KEY_DESERIALIZER);
            consumerConfigProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,KAFKA_VALUE_DESERIALIZER);
            consumerConfigProps.put(ConsumerConfig.GROUP_ID_CONFIG,KAFKA_CONSUMER_GROUP);
            sampleConsumer=new KafkaConsumer<String, String>(consumerConfigProps);
        }

        public void run(){
            sampleConsumer.subscribe(Arrays.asList(topicName));
            System.out.println("Kafka Consumer Subscribed to Topic "+topicName);
            try{
                while (true) {
                    ConsumerRecords<String, String> records = sampleConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records)
                        System.out.println("Message Received: "+record.value());
                }
            }catch(WakeupException ex){
                System.out.println("Exception caught " + ex.getMessage());
            }finally{
                sampleConsumer.close();
                System.out.println("Kafka Consumer Closed..");
            }
        }

        public Consumer<String,String> getConsumer(){
            return this.sampleConsumer;
        }
    }

}
