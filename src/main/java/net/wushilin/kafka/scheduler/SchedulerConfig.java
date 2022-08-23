package net.wushilin.kafka.scheduler;

import java.util.Properties;

public class SchedulerConfig {
    private String name;
    private String srcTopic;
    private String destTopic;
    private Properties extractorProperties;
    public String getExtractorClass() {
        return extractorClass;
    }

    public void setExtractorClass(String extractorClass) {
        this.extractorClass = extractorClass;
    }

    private String extractorClass;

    private boolean enabled;

    public String getSrcTopic() {
        return srcTopic;
    }

    public void setSrcTopic(String srcTopic) {
        this.srcTopic = srcTopic;
    }

    public String getDestTopic() {
        return destTopic;
    }

    public void setDestTopic(String destTopic) {
        this.destTopic = destTopic;
    }

    public boolean enabled() {
        return enabled;
    }

    public void setEnabled() {
        setEnabled(true);
    }

    public void disable() {
        setEnabled(false);
    }
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public SchedulerConfig(String name, String srcTopic, String destTopic, String extractorClass, boolean enabled, Properties extractorProps) {
        this.name = name;
        this.srcTopic = srcTopic;
        this.destTopic = destTopic;
        this.enabled = enabled;
        this.extractorClass = extractorClass;
        this.extractorProperties = extractorProps;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "SchedulerConfig{" +
                "name='" + name + '\'' +
                ", srcTopic='" + srcTopic + '\'' +
                ", destTopic='" + destTopic + '\'' +
                ", extractorProperties=" + extractorProperties +
                ", extractorClass='" + extractorClass + '\'' +
                ", enabled=" + enabled +
                '}';
    }

    public Properties getExtractorProperties() {
        return extractorProperties;
    }

    public void setExtractorProperties(Properties extractorProperties) {
        this.extractorProperties = extractorProperties;
    }

    public void setName(String name) {
        this.name = name;
    }
    public String getId() {
        return String.format("scheduler-%s-%s", srcTopic, destTopic);
    }
}
