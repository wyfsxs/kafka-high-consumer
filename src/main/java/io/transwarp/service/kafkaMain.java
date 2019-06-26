package io.transwarp.service;

import io.transwarp.redis.RedisUtil;
import io.transwarp.utils.MD5Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @Function:
 * @Author: create by wyf
 * @Date: 2019/6/20 21:33
 * @Version 1.0
 */
public class kafkaMain {

    private static final Logger logger = LoggerFactory.getLogger(kafkaMain.class);
    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
    private static long startOffset = 0;
    private static String topicStr = "topic_keywords";

    public static void main(String[] args) {

        for (int i = 0; i <= 9; i++) {

            KafkaHighConsumerThread kafkaHighConsumerThread = new KafkaHighConsumerThread("test", i, "topic_keywords") {
                @Override
                public void kafkaHighConsumer(KafkaConsumer<Integer, String> consumer, Set<String> topic, int partition) {
                    try {
                        List<String> topics = new ArrayList<>(topic);
                        if (!topics.isEmpty()) {
                            topicStr = topics.get(0);
                        }
                        //拼接redis的key
                        String redisKeyStr = topicStr + "_" + partition;
                        String redisKey = MD5Util.md5(redisKeyStr);
                        //初始化启动时，获取topic分区的offset值
                        String startOffsetStr = RedisUtil.get(redisKey);
                        if (StringUtils.isNotEmpty(startOffsetStr)) {
                            startOffset = Long.valueOf(startOffsetStr);
                        }
                        //指定消费某个topic的某个分区
                        TopicPartition topicPartition = new TopicPartition(topicStr, partition);
                        consumer.assign(Arrays.asList(topicPartition));
                        //指定消费从指定offset开始消费
                        consumer.seek(topicPartition, startOffset);
                        //消费主进程，多线程需要while(true)，防止重复消费
                        while (true) {
                            ConsumerRecords<Integer, String> records = consumer.poll(1000);
                            for (ConsumerRecord<Integer, String> record : records) {
                                long currentOffset = record.offset() + 1;
                                String value = record.value();
                                System.out.println("message: " + value);
                                System.err.println("offset :" + currentOffset + " parition :" + record.partition());
                                //记录offset的偏移量，用于下面提交指定偏移量
                                currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(currentOffset, "no metadata"));
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
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            consumer.close();
                            System.err.println("Closed consumer and we are done");
                        }
                    }
                }
            };
            kafkaHighConsumerThread.start();
        }
    }
}
