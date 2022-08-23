package net.wushilin.kafka.scheduler;

import net.wushilin.props.EnvAwareProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class CleanUp {
    private static Logger log = LoggerFactory.getLogger(CleanUp.class);
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        Properties p = EnvAwareProperties.fromPath("./example/stream.properties");
        AdminClient adm = AdminClient.create(p);

        int counter = 10;
        for(int i = 0; i < counter; i++) {
            String srcTopic = "src-topic-" + i;
            String destTopic = "dest-topic-" + i;
            String changelog = "kafka-scheduler-01-scheduler-" + srcTopic + "-" + destTopic + "-changelog";
            try {
                System.out.println(adm.deleteTopics(Arrays.asList(srcTopic, destTopic, changelog)).all().get());
            } catch (Exception ex) {
                log.info("Topics may not exist!");
            }
        }
        Thread.sleep(5000);
        for(int i = 0; i < counter; i++) {
            String srcTopic = "src-topic-" + i;
            String destTopic = "dest-topic-" + i;
            String changelog = "kafka-scheduler-01-scheduler-" + srcTopic + "-" + destTopic + "-changelog";
            System.out.println(adm.createTopics(make(srcTopic, destTopic)).all().get());
        }
        java.io.File base = new java.io.File("./statestore");
        Files.walk(base.toPath())
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(java.io.File::delete);
        base.mkdirs();
    }

    public static List<NewTopic> make(String...args) {
        List<NewTopic> result = new ArrayList<>();
        for(String next:args) {
            NewTopic nt = new NewTopic(next, 1, (short)1);
            result.add(nt);
        }
        return result;
    }
}
