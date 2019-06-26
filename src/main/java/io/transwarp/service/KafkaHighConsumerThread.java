package io.transwarp.service;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * @Function: kafka高阶consumer线程写法
 * @Author: create by wyf
 * @Date: 2019/6/23 21:03
 * @Version 1.0
 */
public abstract class KafkaHighConsumerThread extends ShutdownableThread {

    private static final Logger logger = LoggerFactory.getLogger(KafkaHighConsumerThread.class);
    //高阶消费主类
    private KafkaConsumer<Integer, String> consumer;
    //topic的集合
    private Set<String> topics;
    //一次请求最大等待时间
    private static final int waitTime = 1000;
    //brokers连接地址
    private static String bootstrapServers = "bootstrap.servers";
    //group id
    private static String groupId = "group.id";
    //kafka消费起始位置
    private static String autoOffsetReset = "auto.offset.reset";
    //是否自动提交offset
    private static String enableAutoCommit = "enable.auto.commit";
    //zookeeper连接超时时间
    private static String zookeeperSessionTime = "zookeeper.session.timeout.ms";
    //zookeeper数据同步时间
    private static String zookeeperSyncTime = "zookeeper.sync.time.ms";
    //自动提交的超时时间
    private static String autoCommitInterval = "auto.commit.interval.ms";
    //消息key值使用的反序列化类
    private static String keyDeserializer = "key.deserializer";
    //消息value值使用的反序列化类
    private static String valueDeserializer = "value.deserializer";
    //kafka consumer的groupId值
    private String appId;
    //kafka的topic的分区
    private int partition;

    public KafkaHighConsumerThread(String appId, int partition, String... topics) {
        super("kafkaConsumerExample", false);
        Set<String> topicSet = new HashSet<>();
        for (String topic : topics) {
            topicSet.add(topic);
        }
        Properties props = new Properties();
        String fsPath = System.getProperty("user.dir") + "/src/main/resources/";
        System.setProperty("java.security.auth.login.config", fsPath + "kafka_client_jaas.conf");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put(bootstrapServers, "192.168.141.128:9092");  //连接地址
        props.put(groupId, appId);
        props.put(autoOffsetReset, "latest"); //必须要加，如果要读旧数据
        props.put(enableAutoCommit, false);
        props.put(zookeeperSessionTime, "400");
        props.put(zookeeperSyncTime, "200");
        props.put(autoCommitInterval, "1000");
        props.put(keyDeserializer, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(valueDeserializer, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<Integer, String>(props);
        this.topics = topicSet;
        this.appId = appId;
        this.partition = partition;
    }

    @Override
    public void doWork() {
        kafkaHighConsumer(consumer, topics, partition);
    }

    //kafka消费的主方法
    public abstract void kafkaHighConsumer(KafkaConsumer<Integer, String> consumer, Set<String> topic, int partition);
}
