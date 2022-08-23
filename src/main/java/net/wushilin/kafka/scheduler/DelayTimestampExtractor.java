package net.wushilin.kafka.scheduler;

import org.apache.kafka.streams.processor.api.Record;

import java.util.Properties;

/**
 * This simply post the message at xxx ms later.
 *
 * A delay of 0 or negative will post message immedaitely. Otherwise, message will be delayed by "delay" ms.
 *
 * Configure scheduler.xxxx.extractor.delay=500000
 */
public class DelayTimestampExtractor implements PublishTimestampExtractor {
    private long delay = 0L;
    @Override
    public long getPublishAfter(Record<byte[], byte[]> record) {
        if(delay <= 0) {
            return 0;
        } else {
            return System.currentTimeMillis() + delay;
        }
    }

    @Override
    public void init(Properties p) {
        this.delay = Long.parseLong(p.getProperty("delay", "30000"));
    }
}
