package com.deng.kafka.Demo;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MsgProducer {
    private final static String TOPIC_NAME = "my-replicated-topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "192.168.101.61:9092,192.168.101.61:9093,192.168.101.61:9094"
        );

        // 持久化机制
        /**
         *  0. 当ack=0 不需要等待任何broker确认收到消息回复，效率最高。但是容易丢失数据
         *  1. 当ack=1表示等待leader成功的将数据写入到本地log，而不是所有的follower成功写入就可以发送下一条消息。
         *      会出现follower未备份数据，如果leader挂掉，会出现数据丢失的情况
         *  2. 当ack=-1的时候，表示必须要leader和配置文件中的副本都执行备份，才会执行下一个数据
        */
        props.put(ProducerConfig.ACKS_CONFIG,"1");

        // 重试机制，要注意重复发送的幂等性处理
        props.put(ProducerConfig.RETRIES_CONFIG,3);
        //重试间隔
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,300);
        //发送消息的本地缓冲区，发送的消息会先放到本地的缓冲区中
        //默认32M
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,32*1024*1024);
        //kafka本地线程从缓冲区读取数据，批量发送到broker
        //当满足设定的 batch 就从本地发送到broker
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,16*1024);

        //设定 batch的必须发送时间
        //当达到10ms的时候，batch即使没满，也会发送
        props.put(ProducerConfig.LINGER_MS_CONFIG,10);
        //将发送的key从字符串转换成字节数组
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //将发送的value从字符串转换成字节数组
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //创建生产者
        Producer<String,String> producer = new KafkaProducer<String, String>(props);
        int msgNum = 5;
        final CountDownLatch countDownLatch = new CountDownLatch(msgNum);

        for (int i = 1; i <= msgNum; i++) {
            //订单
            Order order = new Order(i,100+i,1,1000.00);

            // ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME,0, order.getOrderId().toString(), JSON.toJSONString(order));


            // 未指定发送分区，具体发送的分区计算公式: hash(key)%partitionNum
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, order.getOrderId().toString(), JSON.toJSONString(order));

            // 等待消息发送成功的一个同步阻塞方法
            RecordMetadata metadata = producer.send(producerRecord).get();
            //当发送成功的时候，才会执行下面的输出，否则就会等待
            System.out.println("concurrent send msg result: "+"Topic-"+metadata.topic()+"|partition-"
                    +metadata.partition()+"|offset-"+metadata.offset()
            );

            // 如果传入了callback 就会异步的执行
            // 在模拟的过程中，需要加入 countDownLatch来阻塞
            producer.send(producerRecord, (metadata1, exception) -> {
                if (exception != null){
                    System.out.println("异步发送失败");
                }
                if (metadata1 != null){
                    System.out.println("concurrent send msg result: "+"Topic-"+metadata1.topic()+"|partition-"
                            +metadata1.partition()+"|offset-"+metadata1.offset()
                    );
                }
                countDownLatch.countDown();
            }).get();
        }

        //等待5s后唤醒
        countDownLatch.await(5, TimeUnit.SECONDS);
        producer.close();





    }

}
