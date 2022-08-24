# kafka-message-scheduler
A KStream application that can schedule kafka messages for delivery later based on condition. Useful for event driven 
system that can't schedule messages out of the box.

# Purpose

The purpose of the library is to schedule a kafka message for delivery later.

It read from a topic A and publish messages to topic B based on condition.

**If a publish target timestamp is specified and is in the future (typically by a header):**

Message will be scheduled and will be available at topic B on or after the timestamp.

**If a publish target timestamp is specified but it is in the past:**

Message will be published immediately.

**If no message publish target timestamp is specified:**

Message will be published immediately.

Default implementation supports extracting target target timestamp from header in different formats. 
You can override this by providing a implementation yourself. See below for more info.

Typical use case:

1. Your API received a request that need a message needs to be available at TopicA at 12:00:00 midnight tonight.

2. You can produce it to TopicB, with a header: `publish-at`: `2022-09-01 00:00:00`.

3. Then you can run the kafka-message-scheduler (this program) to copy data in TopicB to TopicA with config to 
recognize the `publish-at` header so message can be published immediately past the timestamp.


# Features
## Exactly once capable. (You have to configure EOS)
This is based on your configuration in stream.properties below.

If you configured your EOS, then you must configure the consumer of copied topic to use `read_comitted` isolation level.

## High throughput: Throughput up to 500K messages per thread per 5 seconds
If this is not enough, please add more threads and partitions.

## Message order guarantee
Messages will be published in the following order:
1. All messages that are supposed to publish immediately, are published in order of reading.
2. All messages that are scheduled to publish in the future,  are published in order of the following:
    1. The scheduled time: Messages with earlier scheduled time is published earlier
    2. Messages scheduled at the same time in millisecond (e.g. `midnight today`), are published in order of the original topic.
    3. The order is preserved in general.

## Multi scheduling
You can add many scheduler to publish multiple topics to multiple topics.

## Data preservance
Keys are copied byte for byte.

Values are copied byte for byte.

Headers are preserved. The order of header may be affected, however that typically will not cause any issue.

The copy process does not touch the data at all.

## Encrypted payload
Encrypted payload is well supported. This is because the headers are copied, and the encrypted payload is untouched.


## Schema enabled data types
Avros, JSON, Protobuffs, are supported. This is because the encoded data has the schema id encoded, which will
be copied as is. 

However, if you view the schema in your control center, or any other tool you will not see the subject defined
for the new topic. This is normal. Your Schema enabled consumer should be able to consume the data as if you
were consuming from the original topic.

# Building the software
```bash
$ gradle clean jar
```

Check executable jar in build/libs/KafkaScheduler-xxxx.jar

# Configuration
## log4j.properties
You have to create a log4j.properties to run the program. Please refer to examples/log4j.properties

This program uses reload4j, so it is free from the log4j vulunerability.

Example:
```properties
log4j.rootLogger=INFO, consoleAppender
log4j.appender.consoleAppender=org.apache.log4j.ConsoleAppender
log4j.appender.consoleAppender.layout=org.apache.log4j.PatternLayout
log4j.appender.consoleAppender.layout.ConversionPattern=%d - [%t] %-5p %c %x - %m%n
```

## stream.properties
You have to create a `stream.properties`, or whatever name you prefer.

It must contain the following keys:
1. Scheduler name list
```properties
# The scheduler of these names will be loaded.
scheduler.list=name1,name2,name3,name4,....,nameN
```
 
4. Scheduler definiation for above names. Supported keys are:
```properties
# The source topic name for this scheduler
scheduler.name1.topic.src=source-topic
# The destination topic name for this scheduler
scheduler.name1.topic.dest=destination-topic
# The header name to get the publish timestamp. Default: publish-after-ts
scheduler.name1.extractor.headerName=after-ts
# The timestamp format for the header. Valid values are 
# long -> time in seconds since EPOCH (System.currentTimeMillis() / 1000)
# longms -> time from System.currentTimeMillis()
# java.text.SimpleDateFormat acceptable format string
scheduler.name1.extractor.timestampFormat=yyyy-MM-dd HH:mm:ss
# Scheduler can be disabled. In this case, they won't be run.
# This defaults to true
scheduler.name1.enable=true

# If you included multiple scheduler.list in the `scheduler.list` key, you have to define them all here
scheduler.name2.topic.src=src-topic-1
scheduler.name2.topic.dest=dest-topic-1
# You can implement net.wushilin.kafka.scheduler.PublishTimestampExtractor for customized behavior of publish time.
# See DelayTimestampExtractor for reference. It simply add a delay to all messages. Delay is configured as 
# 1 minute as shown below
scheduler.name2.extractor-class=net.wushilin.kafka.scheduler.DelayTimestampExtractor
scheduler.name2.extractor.delay=60000

# More scheduler definition below
```

It should contain any supported Kafka Stream configuration options
```properties
# Required connection configs for Kafka producer, consumer, and admin
bootstrap.servers=single-kafka-1.lxd:9092
security.protocol=PLAINTEXT
acks=all
compression.type=zstd
enable.idempotence=true
batch.size=300000
linger.ms=5

# Stream settings
application.id=kafka-scheduler-01

# Must be ByteArraySerde
default.key.serde=org.apache.kafka.common.serialization.Serdes$ByteArraySerde
# Must be ByteArraySerde
default.value.serde=org.apache.kafka.common.serialization.Serdes$ByteArraySerde

# Change for other setting for intermediate topic used for statestore
replication.factor=1

# Processing guarantee
processing.guarantee=exactly_once_v2

# Threads to run
num.stream.threads=20

#Commit interval (default is 100 if using EOS or 30000 if not usign EOS)
commit.interval.ms=100

# State store directory. It may require quite some disk space
state.dir=./statestore

# Recommend to set a large enough value, otherwise transaction may fail and retried duing peak load.
# Should not exceed server max setting, which is 15 minutes typically.
# If not set, this is 10 seconds, which is very short!!!
transaction.timeout.ms=60000
```

## Splitting config files
You can put the properties of `stream.properties` in any number of config files.

You can specify path to the files in command argument. All of them will be loaded in order.

If conflicts found (e.g. a same key specified in multiple properties, earlier definition will take effect.)

# Configuration example
Please refer `examples/log4j.properties`, `examples/scheduler.properties`, `example/stream.properties`


# Running
In a single config file `stream.properties`:

```bash
$ java -Dlog4j.configuration=file:/path/to/log4j.properties -jar build/libs/KafkaScheduler-1.0-SNAPSHOT.jar /path/to/stream.properties
```

If you have split the config files into multiple files (just like the examples):
```bash
$ java -Dlog4j.configuration=file:/path/to/log4j.properties -jar build/libs/KafkaScheduler-1.0-SNAPSHOT.jar /path/to/stream.properties /path/to/scheduler.properties /path/to/other.properties ...
```

# Running as systemd
The following systemd unit file may help you to get started.

```systemd
[Unit]
Description=The Kafka Scheduler
After=network.target

[Service]
User=appuser01
Group=appuser01
Type=simple
WorkingDirectory=/opt/services/kafka-message-scheduler
ExecStart=/usr/bin/java -Xmx512m -Dlog4j.configuration=file:/path/to/log4j.properties -jar build/libs/KafkaScheduler-1.0-SNAPSHOT.jar /path/to/stream.properties /path/to/scheduler.properties /path/to/other.properties
Restart=always
RestartSec=3
SyslogIdentifier=kafka-message-scheduler

[Install]
WantedBy=multi-user.target
```

Edit and place the file in `/etc/systemd/system/kafka-message-scheduler.service`

Reload systemd config:
```bash
# systemctl daemon-reload
```

Enable the service
```bash
# systemctl enable kafka-message-scheduler
```

Start the service
```bash
# systemctl start kafka-message-scheduler
```

Check your log based on your log4j.properties for any error.

# Fixing issues
Occasionally, you may accidentally publish a lot of messages that are supposed to publish in very far future (e.g. 2095-01-01 by mistake).

These messages can be deleted from the scheduled topic by using delete.water.mark.

You may start your program with additional argument -Ddelete.water.mark=4800000000000 (equals to Sun Feb 08 21:20:00 SGT 2122).

This will delete all records that has the scheduled publish time <= than this date.
