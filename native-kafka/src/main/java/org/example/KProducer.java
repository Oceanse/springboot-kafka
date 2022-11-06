package org.example;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class KProducer {

    //没有topic会自动创建
    private final static String TOPIC_NAME = "my-replicated-topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //syncSent();
        asyncSent();

    }

    /**
     * 生产者同步向集群发送消息
     * 生产者发送消息后会等待ack,没有等到就会阻塞；阻塞时间为3s,3s后还没收到ack,就会进行重试，重试次数为3；3次后还没收到就会产生异常
     * <p>
     * 使用命令行让消费者进行消费，可以收到这里生产者发送的消息
     * ./kafka-console-consumer.sh --bootstrap-server 192.168.1.4:9091,192.168.1.4:9092,192.168.1.4:9093 --from-beginning --topic my-replicated-topic
     */
    public static void syncSent() {
        Properties props = new Properties();

        //broker集群
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.4:9091,192.168.1.4:9092,192.168.1.4:9093");

        //把发送的key从字符串序列化为字节数组
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //把发送消息value从字符串序列化为字节数组
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //生产者客户端用于缓存消息缓冲区(Record Accumulator)大小。
        props.put("buffer.memory", 33554432);//32MB

        //缓冲区数据达到batchSize或等待时间超过linger.ms值时就会发送出去
        props.put("batch.size", 16384);//16KB

        /**
         * 首先这个acks参数，是在KafkaProducer，也就是生产者客户端里设置的,也就是说，你往kafka写数据的时候，就可以来设置这个acks参数
         * acks=0:
         * 表示producer不需要等待任何broker确认收到消息的回复，就可以继续发送下一条消息。KafkaProducer在客户端，只要把消息发送出去，
         * 不管那条数据有没有在哪怕Partition Leader上落到磁盘，我就不管他了，直接就认为这个消息发送成功了。如果你采用这种设置的话，那么你必须
         * 注意的一点是，可能你发送出去的消息还在半路。结果呢，Partition Leader所在Broker就直接挂了，然后结果你的客户端还认为消息发送成功了，
         * 此时就会导致这条消息就丢失了。
         *
         * acks=1：
         * 只要Partition Leader接收到消息而且写入本地磁盘了，就认为成功了，不管他其他的Follower有没有同步过去这条消息了, 就可以继续发送下一条消息。
         * 这种设置其实是kafka默认的设置,但是这里有一个问题，万一Partition Leader刚刚接收到消息，Follower还没来得及同步过去，结果Leader所在的
         * broker宕机了，此时也会导致这条消息丢失，因为人家客户端已经认为发送成功了。
         *
         * acks=-1或all：
         * Partition Leader接收到消息之后，还必须要求ISR列表里跟Leader保持同步的那些Follower都要把消息同步过去，才能认为这条消息是写入成功了。
         * 如果说Partition Leader刚接收到了消息，但是结果Follower没有收到消息，此时Leader宕机了，那么kafka就不会发送acks, 客户端会感知到这个消息没发送成功，
         * 他会重试再次发送消息过去。这是最强的数据保证。一般除非是金融级别，或跟钱打交道的场景才会使用这种配置。
         */
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        //创建一个要发送的消息对象，并带上配置
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
        for (int i = 0; i < 30; i++) {
            Order order = new Order((long) i, i);
            /**
             * TOPIC_NAME: 决定发送的主题topic
             * key: 决定发送的分区partition，分区计算公式：hash(key)%partitionNum，也就是order.getOrderId().toString()
             * value:发送的消息，也就是JSON.toJSONString(order)
             */
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, order.getOrderId().toString(), JSON.toJSONString(order));

            //发送消息，返回消息的元数据并输出
            RecordMetadata metadata = null;
            try {
                /**
                 *  future.get会进行阻塞直到返回数据表示发送成功，才会继续下一条消息的发送；
                 *  此方式如果发送失败会进行重试，直至重试达到retries最大次数，
                 *  达到最大次数后还没收到ack就会产生异常,默认重试3次,此方式也是最大程度确保数据可靠性，
                 */
                metadata = producer.send(producerRecord).get();
            } catch (InterruptedException e) {
                //重新尝试或者计入日志
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
            System.out.println("同步方式发送消息结果：" + "topic-" + metadata.topic() + "|partition-" + metadata.partition() + "|offset-" + metadata.offset());
        }
        producer.close();
    }


    /**
     * 生产者异步向集群发送消息
     * 生产者发消息，发送完后不用等待broker给回复，直接执行下面的业务逻辑。可以提供callback，让broker异步的调用callback，告知生产者，消息发送的结果
     */
    public static void asyncSent() throws InterruptedException {
        Properties props = new Properties();

        //broker集群
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.4:9091,192.168.1.4:9092,192.168.1.4:9093");

        //把发送的key从字符串序列化为字节数组
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //把发送消息value从字符串序列化为字节数组
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //设置acks
        props.put(ProducerConfig.ACKS_CONFIG, "0");

        //创建一个要发送的消息对象，并带上配置
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
        // 门栓
        CountDownLatch countDownLatch = new CountDownLatch(30);
        for (int i = 0; i < 30; i++) {
            Order order = new Order((long) i, i);
            /**
             * TOPIC_NAME: 决定发送的主题topic
             * key: 决定发送的分区partition，分区计算公式：hash(key)%partitionNum，也就是order.getOrderId().toString()
             * value:发送的消息，也就是JSON.toJSONString(order)
             */
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, order.getOrderId().toString(), JSON.toJSONString(order));

            /**
             * 在使用send方法时指定一个回调函数，服务器在响应时会调用此函数，通过回调函数对结果进行处理，可以直到消息是写成功还是失败，
             * 异步加回调效率高，也可以异步知道消息发送结果，缺点是无法重试
             */
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        System.err.println("发送消息失败：" + e.getStackTrace());
                    }
                    if (recordMetadata != null) {
                        System.out.println("异步方式发送消息结果：" + "topic-" + recordMetadata.topic() + "|partition-"
                                + recordMetadata.partition() + "|offset-" + recordMetadata.offset());
                    }
                    countDownLatch.countDown();
                }
            });
        }

        // 当countDownLatch变成0之前，一直阻塞
        countDownLatch.await();
        producer.close();
    }
}

