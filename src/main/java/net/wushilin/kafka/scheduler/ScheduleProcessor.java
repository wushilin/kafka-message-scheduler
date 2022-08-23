package net.wushilin.kafka.scheduler;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class ScheduleProcessor implements Processor<byte[], byte[], byte[], byte[]> {
    private static Random rand = new Random();
    private Logger logger = LoggerFactory.getLogger(ScheduleProcessor.class);
    private KeyValueStore<String, byte[]> kvStore;
    private ProcessorContext<byte[], byte[]> context;
    private AtomicLong serial = new AtomicLong(0L);
    private static final long LIMIT = 500000L;
    private PublishTimestampExtractor extractor;

    private SchedulerConfig config = null;

    private String nextID() {
        int digits = 10;
        long sequence = serial.addAndGet(1L) % pow(10L, digits);
        String seq = String.format("%0"+digits+"d", sequence);
        return seq;
    }

    static long pow (long a, int b)
    {
        if ( b == 0)        return 1;
        if ( b == 1)        return a;
        if (b % 2 == 0)    return     pow ( a * a, b/2); //even a=(a^2)^b/2
        else                return a * pow ( a * a, b/2); //odd  a=a*(a^2)^b/2

    }

    public ScheduleProcessor(SchedulerConfig config) {
        this.config = config;
    }

    @Override
    public void init(ProcessorContext<byte[], byte[]> context) {
        this.context = context;
        Processor.super.init(context);
        kvStore = context.getStateStore(config.getId());
        String extractorClass = config.getExtractorClass();
        if(extractorClass != null) {
            try {
                extractorClass = extractorClass.trim();
                this.extractor = (PublishTimestampExtractor) Class.forName(extractorClass).getDeclaredConstructor().newInstance();
                this.extractor.init(config.getExtractorProperties());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        context.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            long counter = 0;
            Date start = new Date();
            long roughCount = kvStore.approximateNumEntries();
            if(roughCount > 0) {
                logger.info("Approximately {} records in flight.", roughCount);
            }
            try (final KeyValueIterator<String, byte[]> iter = kvStore.all()) {
                while (iter.hasNext()) {
                    final KeyValue<String, byte[]> entry = iter.next();
                    String keyS = entry.key;
                    String[] tokens = keyS.split(",");
                    long key = Long.parseLong(tokens[0]);
                    String id = tokens[1];
                    long now = System.currentTimeMillis();
                    if (now >= key) {
                        //logger.info("Firing for key {} -> value {}, timestamp {}", new String(record.getKey()), new String(record.getValue()), timestamp);
                        Record<byte[], byte[]> theRecord = RecordSerializer.deserialize(entry.value);
                        context.forward(theRecord);
                        //logger.info("Deleting key for {}", new Date(key));
                        kvStore.delete(keyS);
                        if (kvStore.get(keyS) != null) {
                            throw new IllegalStateException("Why deleted keys are still there?");
                        }
                        counter++;
                        if (counter >= LIMIT) {
                            logger.info("Breaking at limit of {}", LIMIT);
                            break;
                        }
                    } else {
                        break;
                    }
                }
                if(counter > 0) {
                    logger.info("Processed {} records.", counter);
                }
                //context.commit();
            }
        });
    }

    private static long parseDateLong(String value, String format) {
        if ("long".equalsIgnoreCase(format)) {
            return Long.parseLong(value) * 1000;
        }
        if ("longms".equalsIgnoreCase(format)) {
            return Long.parseLong(value);
        }

        SimpleDateFormat sdf = getDateFormat(format);
        try {
            Date dt = sdf.parse(value);
            return dt.getTime();
        } catch (Exception ex) {
            return 0;
        }
    }

    private static SimpleDateFormat getDateFormat(String format) {
        return new SimpleDateFormat(format);
    }

    @Override
    public void process(Record<byte[], byte[]> record) {
        long cutover = extractor.getPublishAfter(record);
        long now = System.currentTimeMillis();
        if (now < cutover) {
            int loopCount = 0;
            while (true) {
                String randomId = nextID();
                String key = "" + cutover + "," + randomId;
                byte[] sv = kvStore.get(key);
                if (sv == null) {
                    if (loopCount > 0) {
                        logger.info("Unlikely event: {} has a clash. {} loops resolved the issue.", key, loopCount);
                    }
                    //logger.info("Scheduled at a free slot: {}", key);
                    sv = RecordSerializer.serialize(record);
                    kvStore.put(key, sv);
                    if (kvStore.get(key) == null) {
                        throw new IllegalStateException("Why put key is not present?");
                    }
                    return;
                } else {
                    //Unfortunate event of a clash. Trying again!
                    loopCount++;
                    continue;
                }
            }
        } else {
            context.forward(record);
        }
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
