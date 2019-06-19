package io.transwarp.kafka;


import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.util.Properties;

/**
 * @function: kafka消费者工具类
 * @auther: Create by wyf
 * @date: 2018/11/12
 * @version: v1.0
 */
public class KafkaConsumerUtil {

    /**
     * kafka高阶消费工具类
     *
     * @return
     */
    public KafkaConsumer<Integer, String> kafkaConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.141.128:9092");  //连接地址
        props.put("group.id", "test");
        props.put("auto.offset.reset", "latest"); //必须要加，如果要读旧数据
        props.put("enable.auto.commit", false);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(props);
        return consumer;
    }

}
