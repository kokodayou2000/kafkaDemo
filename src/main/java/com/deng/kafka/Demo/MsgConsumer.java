package com.deng.kafka.Demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import sun.rmi.runtime.Log;

import java.time.Duration;
import java.util.*;

public class MsgConsumer {
    private final static String TOPIC_NAME = "my-replicated-topic";
    private final static String CONSUMER_GROUP_NAME ="tGroup";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "192.168.101.61:9092,192.168.101.61:9093,192.168.101.61:9094"
                );
        // 消费分组名
        props.put(ConsumerConfig.GROUP_ID_CONFIG,CONSUMER_GROUP_NAME);
        //是否自动提交 offset
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        //自动提交offset的间隔时间
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000")
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");

        /**
         * 当Topic是一个新的消费组的时候，或指定 offset 的消费方式， offset 不存在，应该如何消费
         * latest():只消费自己启动之后发送到主题的消息
         * earliest(): 从头开始消费，以后按照消费的offset记录继续消费，
         * 这个和 consumer.seekToBeginning不同，consumer.seekToBeginning每次都会从头开始
         */
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        /**
         * consumer 给 broker发送心跳的间隔时间，broker接收到心跳，如果此时有 rebalance 发生，
         * 消费者会将rebalance方案下发给consumer
         */
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,1000);

        /**
         * 服务端broker多久感知不到consumer心跳，就会认为他挂掉了，然后将其从消费者中删除
         * 对应的分区(partition)也会被重新分配给其他的consumer，默认10s
         */

        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,10*1000);

        // 一次poll最大拉取消息的条数，如果消费者处理的速度很快，可以设置的大一点，如果消费者处理的速度一般，就设置的小一点
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,50);

        /**
         * 如果两次拉取消息的时间间隔超过了该值，broker就会认为这个consumer处理能力有问题（网络故障，机器运转等问题）
         * 消费者就会将其踢出消费组，并将这个分区交给别的consumer消费
         */
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,30*1000);

        //将key字符数组转换成序列号字符串
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //将value字符数组转换成序列号字符串
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 消费者订阅该主题
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        // 消费组指定分区,指定了0分区
//        consumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME,0)));

        // 每次消费组都会从头开始消费
//        consumer.seekToBeginning(Arrays.asList(new TopicPartition(TOPIC_NAME,0)));

        // 每次消费者都会从指定的偏移量进行消费,指定从第3条消息开始
//        consumer.seek(new TopicPartition(TOPIC_NAME,0),3);

        /*
//        指定时间点开始消费
        // 获取所有的分区
        List<PartitionInfo> topicPartitions = consumer.partitionsFor(TOPIC_NAME);
        // 指定时间为一小时前
        long fetchDateTime = new Date().getTime() - 1000 * 60 * 60;
        HashMap<TopicPartition, Long> map = new HashMap<>();
        // 主题分区和时间  map 映射表为
        // [key:{"my-replicated-topic",0},value:before a hour]
        // [key:{"my-replicated-topic",1},value:before a hour]
        for (PartitionInfo part: topicPartitions){
            map.put(new TopicPartition(TOPIC_NAME,part.partition()),fetchDateTime);
        }
        // 会比对时间戳，然后保留指定时间戳之后的分区
        Map<TopicPartition, OffsetAndTimestamp> parMap = consumer.offsetsForTimes(map);
        for (Map.Entry<TopicPartition,OffsetAndTimestamp> entry : parMap.entrySet()){
            TopicPartition key = entry.getKey();
            OffsetAndTimestamp value = entry.getValue();
            if (key == null || value == null) continue;
            long offset = value.offset();
            System.out.println("partition-"+key.partition()+"|offset-"+offset);
            System.out.println();
            //根据消费的时间来确定偏移量
            consumer.assign(Collections.singletonList(key));
            // 根据偏移量来执行
            consumer.seek(key,offset);
        }
         */

        while (true){
            /*
              poll() 拉取消息发长轮询
             */
            //1000ms拉取一次
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("接收到的消息： 分区 = {}， 偏移量 = {}，key = {}， value = {} \n%n", record.partition(), record.offset(), record.key(), record.value());
            }
            if (records.count() > 0){
                // 手动提交offset，当前线程会阻塞，直到offset提交成功  常用
                consumer.commitAsync();
                // 手动异步提交 offset，当前线程提交 offset 不会阻塞，可以继续处理后面的程序逻辑
                // OffsetCommitCallback
                consumer.commitAsync((offsets, exception) -> {
                    if (exception != null){
                        System.out.println("Commit filed for "+ offsets);
                        System.out.println("exception --> "+Arrays.toString(exception.getStackTrace()));
                    }
                });

            }

        }



    }

}
