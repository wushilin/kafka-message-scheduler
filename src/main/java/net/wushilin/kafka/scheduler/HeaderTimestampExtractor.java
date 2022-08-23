package net.wushilin.kafka.scheduler;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class HeaderTimestampExtractor implements PublishTimestampExtractor{
    private static Logger log = LoggerFactory.getLogger(HeaderTimestampExtractor.class);
    private String timestampFormat = "longms";
    private String headerName = "publish-after-ts";
    public static ThreadLocal<SimpleDateFormat> formatCache = new ThreadLocal<>();
    public HeaderTimestampExtractor() {
    }
    public HeaderTimestampExtractor(String headerName, String format) {
        this.timestampFormat = format;
        this.headerName = headerName;
    }

    @Override
    public long getPublishAfter(Record<byte[], byte[]> record) {
        Header h = record.headers().lastHeader(headerName);
        if(h == null) {
            return 0;
        }
        byte[] valueBytes = h.value();
        if(valueBytes == null || valueBytes.length == 0) {
            return 0;
        }


        String value = new String(valueBytes, StandardCharsets.UTF_8);
        if("longms".equalsIgnoreCase(this.timestampFormat)) {
            try {
                return Long.parseLong(value);
            } catch(Exception ex) {
                log.warn("Failed to parse date for {} with format {}", value, this.timestampFormat);
                return 0;
            }
        }
        if("long".equalsIgnoreCase(this.timestampFormat)) {
            try {
                return Long.parseLong(value) * 1000;
            } catch(Exception ex) {
                log.warn("Failed to parse date for {} with format {}", value, this.timestampFormat);
                return 0;
            }
        }

        try {
            SimpleDateFormat sdf = getFormat();
            return sdf.parse(value).getTime();
        } catch(Exception ex) {
            log.warn("Failed to parse date for {} with format {}", value, this.timestampFormat);
            return 0;
        }
    }

    private SimpleDateFormat getFormat() {
        SimpleDateFormat buffer = formatCache.get();
        if(buffer != null) {
            return buffer;
        }
        SimpleDateFormat newFormat = new SimpleDateFormat(this.timestampFormat);
        formatCache.set(newFormat);
        return newFormat;
    }
    @Override
    public void init(Properties p) {
        this.timestampFormat = p.getProperty("timestampFormat", timestampFormat);
        this.headerName = p.getProperty("headerName", headerName);
    }
}
