package net.wushilin.kafka.scheduler;

import org.apache.kafka.streams.processor.api.Record;

import java.util.Properties;

/**
 * Given a record, extract the target timestamp
 */
public interface PublishTimestampExtractor {
    /**
     * Extract the target publish timestamp in milliseconds since EPOCH
     * @param record The kafka stream record. You may acces the key, value in bytes, timestmap in long and header values
     * @return If desired to publish now, return a timestamp <= current timestamp. Otherwise, return a future timestamp.
     */
    long getPublishAfter(Record<byte[], byte[]> record);

    /**
     * Initialize the properties extractor with the properties
     * @param p The properties for the extractor
     */
    void init(Properties p);
}
