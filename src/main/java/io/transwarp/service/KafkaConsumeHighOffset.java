package io.transwarp.service;

import io.transwarp.kafka.KafkaConsumerUtil;
import io.transwarp.redis.RedisUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.Future;

public class KafkaConsumeHighOffset {

    private KafkaConsumerUtil kafkaConsumer = new KafkaConsumerUtil();
    private KafkaConsumer<Integer, String> consumer;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
    private String topic = "topic_keywords";
    private int partition = 1;
    private long startOffset = 0;
    private String redisKey = topic + "_" + partition;

    public void consumer() {
        try {
            //初始化启动时，获取topic分区的offset值
            String startOffsetStr = RedisUtil.get(redisKey);
            if (StringUtils.isNotEmpty(startOffsetStr)) {
                startOffset = Long.valueOf(startOffsetStr);
            }
            consumer = kafkaConsumer.kafkaConsumer();
            //指定消费某个topic的某个分区
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            consumer.assign(Arrays.asList(topicPartition));
            //指定消费从指定offset开始消费
            consumer.seek(topicPartition, startOffset);
            //消费主进程
            while (true) {
                ConsumerRecords<Integer, String> records = consumer.poll(1000);
                for (ConsumerRecord<Integer, String> record : records) {
                    long currentOffset = record.offset() + 1;
                    String value = record.value();
                    System.out.println("message: " + value);
                    System.err.println("offset :" + currentOffset);
                    //记录offset的偏移量，用于下面提交指定偏移量
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(currentOffset, null));
                    //记录分区消费当前的offset值
                    RedisUtil.set(redisKey, String.valueOf(currentOffset));
                }
                //提交指定的offset
                consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                    if (exception != null) {
                        System.err.println("Failed to commit offsets: " + offsets + "; " + exception.getMessage());
                    }
                });
            }
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e);
        } finally {
            try {
                consumer.commitSync(currentOffsets);
            } finally {
                consumer.close();
                System.err.println("Closed consumer and we are done");
            }

        }
    }

    public void kafkaProduce(String message) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.141.128:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> procuder = new KafkaProducer<String, String>(props);
        ProducerRecord<String, String> msg = new ProducerRecord<String, String>("topic_keywords", message);
        Future<RecordMetadata> send = procuder.send(msg);
        System.out.println(send);
        procuder.close();
    }

    public static void main(String[] args) {
        new KafkaConsumeHighOffset().consumer();
    }

}
