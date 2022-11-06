package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class KConsumer {
    private final static String TOPIC_NAME = "my-replicated-topic";
    private final static String CONSUMER_GROUP_NAME = "testGroup";

    public static void main(String[] args) {
        Properties props = new Properties();
        //brokers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.4:9091,192.168.1.4:9092,192.168.1.4:9093");
        //消费分组名名
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
        //序列化
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 消费者poll到消息后默认情况下，会自动向broker的_consumer_offsets主题提交当前消费者组-主题-分区消费的偏移量,
        // 也就是提交自己的消费消息偏移量，那么下个消费者就会从提交的偏移量+1开始消费；
        // 自动提交：消费者拿到消息后就会提交(确认)，不管有没有消费完； 也就是consumer.poll后就自动提交，不管后面有没有消费成功
        // 自动提交会丢消息：如果消费者还没消费完poll下来的消息就自动提交了偏移量，那么此 时消费者挂了，于是下一个消费者会从已提交的offset的下一个位置开始消费消息。之前未被消费的消息就丢失掉了。
        //手动提交：消费者在消费完消息后进行手动提交；
        //手动和自动的主要区别体现在提交的时间点
        //这里默认就是true，也就是自动提交，设置为fase为手动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 自动提交offset的间隔时间
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        /**
         * latest: 当一个新的消费者组消费该主题时，默认会从current-offset(最后一条被消费消息的偏移量)+1开始消费，
         * 意味着只能消费自己启动后发送到主题的消息，不能消费启动之前该主题中的消息；
         * earliest: 第一次会头开始消费，之后会按照消费offset开始消费，需注意区别consumer.seekToBeginning
         */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //创建一个消费者的客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        // 消费者订阅主题列表
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        // consumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME, 0 ))); //还能指定分区消费

       // consumer.seekToBeginning(Arrays.asList(new TopicPartition(TOPIC_NAME,0 )));  //从头开始消费（回溯消费）


        //指定offset消费:topic+partition+offset
       // consumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME, 0 )));
        //consumer.seek(new TopicPartition(TOPIC_NAME, 0 ), 10 );

        while (true) {
            //poll() API 是拉取消息的⻓轮询
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("收到消息：partition = %d,offset = %d, key =%s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
            }
            //前面声明了手动提交，这里就可以采取手动提交，具体可以使用同步手动提交和异步手动提交
            if(records.count()>0){
                //1 同步手动提交offset,集群返回ack之前当前线程会一直阻塞，返回ack表示提交成功
               // consumer.commitSync();

                //2 异步手动提交offset
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                        if(e!=null){
                            System.err.println("commit failed for "+offsets);
                            e.printStackTrace();
                        }
                    }
                });
            }
        }
    }
}
