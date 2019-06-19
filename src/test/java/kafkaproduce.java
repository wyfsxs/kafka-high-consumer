import io.transwarp.service.KafkaConsumeHighOffset;

/**
 * @Function:
 * @Author: create by wyf
 * @Date: 2019/6/19 15:09
 * @Version 1.0
 */
public class kafkaproduce {


    public static void main(String[] args) {
        for (int i = 1001; i <= 2000; i++) {
            String mess = " this is kafka message: " + i;
            new KafkaConsumeHighOffset().kafkaProduce(mess);
        }
    }
}
