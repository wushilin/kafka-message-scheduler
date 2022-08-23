package net.wushilin.kafka.scheduler;

import net.wushilin.props.EnvAwareProperties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class KafkaScheduler {
    private static Logger log = LoggerFactory.getLogger(KafkaScheduler.class);
    private static List<SchedulerConfig> loadConfig(Properties p) {
        String schedulerList = p.getProperty("scheduler.list");
        List<SchedulerConfig> result = new ArrayList<>();
        if(schedulerList == null || schedulerList.trim().isBlank()) {
            return result;
        }

        String[] tokens = schedulerList.split(",");
        for(String nextScheduler:tokens) {
            nextScheduler = nextScheduler.trim();
            SchedulerConfig toAdd = loadScheduler(p, nextScheduler);
            if(toAdd != null) {
                result.add(toAdd);
            } else {
                log.warn("Ignored invalid scheduler config: {}", nextScheduler);
            }
        }
        return result;
    }

    private static SchedulerConfig loadScheduler(Properties p, String name) {
        String prefix = "scheduler." + name + ".";
        String srcTopic = p.getProperty(prefix + "topic.src");
        String destTopic = p.getProperty(prefix + "topic.dest");
        String enableString = p.getProperty(prefix + "enable");
        String extractorClass = p.getProperty(prefix + "extractor-class",
                "net.wushilin.kafka.scheduler.HeaderTimestampExtractor");
        if(srcTopic != null && destTopic != null) {
            boolean enable = !("false".equalsIgnoreCase(enableString));
            String epPrefix = prefix + "extractor.";
            Properties ep = split(p, epPrefix);
            return new SchedulerConfig(name, srcTopic, destTopic, extractorClass, enable, ep);
        } else {
            log.warn("Please Check your config. " +
                    "Prefix: " + prefix + ", config key: 'topic.src', 'topic.dest', 'timestamp.format', " +
                    "'timestamp.header-name' are required. 'enable' is optional (default true)");
            return null;
        }
    }

    private static Properties split(Properties parent, String prefix) {
        Properties p = new Properties();
        parent.entrySet().forEach( i -> {
            String key = (String) i.getKey();
            String value = (String)i.getValue();
            if(key!=null && key.startsWith(prefix)) {
                p.put(key.substring(prefix.length()), value);
            }
        });
        return p;
    }
    public static void validate(List<SchedulerConfig> keys) {
        Map<String, String> ids = new HashMap<>();
        for(SchedulerConfig next:keys) {
            if(!next.enabled()) {
                continue;
            }
            String id = next.getId();
            if(ids.containsKey(id)) {
                throw new IllegalArgumentException("Conflicting id: " + next.getName()
                        + " conflicts with another scheduler ("+ids.get(id)+")");
            }
            ids.put(id, next.getName());
        }
    }
    public static void main(String[] args) throws IOException {
        Properties p = EnvAwareProperties.fromPath("./example/client.properties", "./example/scheduler.properties");
        Topology builder = new Topology();
        // add the source processor node that takes Kafka topic "source-topic" as input
        List<SchedulerConfig> configs = loadConfig(p);
        validate(configs);
        int counter = 0;
        for(SchedulerConfig next:configs) {
            if(!next.enabled()) {
                log.info("Ignored disabled scheduler {}", next.getName());
                continue;
            }
            log.info("Adding scheduler {} -> {}", next.getName(), next);
            builder.addSource("Source" + counter, next.getSrcTopic())
                    // add the WordCountProcessor node which takes the source processor as its upstream processor
                    .addProcessor("Process" + counter, () -> new ScheduleProcessor(next), "Source" + counter)
                    // add the count store associated with the WordCountProcessor processor
                    .addStateStore(storeBuilderFor(next.getId()), "Process" + counter)
                    // add the sink processor node that takes Kafka topic "sink-topic" as output
                    // and the WordCountProcessor node as its upstream processor
                    .addSink("Sink" + counter, next.getDestTopic(), "Process" + counter);
            counter++;
        }

        if(counter == 0) {
            log.error("No scheduler configured to run!");
            System.exit(1);
        }
        KafkaStreams streams = new KafkaStreams(builder, p);
        streams.start();
    }
    public static StoreBuilder storeBuilderFor(String name) {
        StoreBuilder kvStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(name),
                        Serdes.String(),
                        Serdes.ByteArray()
                );
        return kvStoreBuilder;
    }
}
