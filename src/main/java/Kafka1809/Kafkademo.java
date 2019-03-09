package Kafka1809;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Kafkademo {
    @Test
    public void producer() throws InterruptedException, ExecutionException {
        Properties props=new Properties();
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.42.148.199:9092");

        Producer<Integer, String> kafkaProducer = new KafkaProducer<Integer, String>(props);
        for(int i=0;i<100;i++){
//            创建消息记录对象，指定主题名和生成的数据信息
            ProducerRecord<Integer, String> message = new ProducerRecord<Integer, String>("enbook",""+i);
//          --通过
            kafkaProducer.send(message);
        }

        while(true);
    }
//    创建主题

    @Test
    public void create_topic(){
        ZkUtils zkUtils = ZkUtils.apply("hadoop01:2181,hadoop02:2181,hadoop03:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 创建一个单分区单副本名为t1的topic
        AdminUtils.createTopic(zkUtils, "t1", 1, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
        zkUtils.close();
    }


//    创建消费者线程并指定消费者组：
    @Test
    public void consumer_1(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop03:9092");
        props.put("group.id", "consumer-tutorial");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("enbook", "t2"));

        try {
//            --通过poll超时方法，从主题的分区队列获取数据
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, String> record : records)
                    System.out.println("c1消费:"+record.offset() + ":" + record.value());
            }
        } catch (Exception e) {
        } finally {
            consumer.close();
        }
    }

    @Test
    public void consumer_2(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop02:9092");
        props.put("group.id", "consumer-tutorial");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("enbook", "t2"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, String> record : records)
                    System.out.println("c2消费:"+record.offset() + ":" + record.value());
            }
        } catch (Exception e) {
        } finally {
            consumer.close();
        }
    }

}
