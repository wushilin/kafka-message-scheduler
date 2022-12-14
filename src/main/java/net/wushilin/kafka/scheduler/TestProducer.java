package net.wushilin.kafka.scheduler;

import net.wushilin.props.EnvAwareProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class TestProducer {
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static Random rand = new Random();
    static int sleep = 0;
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        Properties p = EnvAwareProperties.fromPath("./example/stream.properties", "./example/others/producer.properties");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        long ts = 1661389788176L;
        for(int i = 0; i < 200000;i++) {
            long now = ts;
            long due = now + rand.nextInt(1000) + 30000;
            for(int j = 0; j < 10; j++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("src-topic-" + j, "key-" + new Date(due) +"@" + i,
                        "value-" + new Date(due) + "@" + i);
                if(j == 9) {
                    record.headers().add("after-ts-long" +j, ("" + System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
                } else if(j == 8) {
                    record.headers().add("after-ts-short" + j, ("" + System.currentTimeMillis()/1000).getBytes(StandardCharsets.UTF_8));
                } else {
                    record.headers().add("after-ts" + j, sdf.format(new Date(due)).getBytes(StandardCharsets.UTF_8));
                }
                for(int k = 0; k < rand.nextInt(5); k++) {
                    String key = RandomUtil.random(5);
                    byte[] value = RandomUtil.random(3).getBytes(StandardCharsets.UTF_8);
                    record.headers().add(key, value);
                }

                producer.send(record);
            }
            //Thread.sleep(1);
            if(sleep > 0) {
                Thread.sleep(sleep);
            }
            if(i % 10000 == 0) {
                System.out.println("Produced 1 record (" + i + ")");
            }
        }
        producer.flush();
        producer.close();

    }
}
