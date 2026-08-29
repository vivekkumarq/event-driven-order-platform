package com.vivek.platform.order.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

/**
 * Dead-letter recoverer that also logs and counts what it parks.
 *
 * <p>The destination is {@code <original-topic>.DLT} with partition {@code -1}, letting the broker
 * pick a partition. Keeping the source partition (the Spring default) breaks as soon as the DLT has
 * fewer partitions than the source topic.
 */
public class MeteredDeadLetterRecoverer extends DeadLetterPublishingRecoverer {

    private static final Logger log = LoggerFactory.getLogger(MeteredDeadLetterRecoverer.class);

    private final MeterRegistry meterRegistry;

    public MeteredDeadLetterRecoverer(KafkaOperations<?, ?> template, MeterRegistry meterRegistry) {
        super(template, (record, exception) -> new TopicPartition(
                record.topic() + KafkaTopicsProperties.DLT_SUFFIX, -1));
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Consumer<?, ?> consumer, Exception exception) {
        meterRegistry.counter("platform.kafka.dlt.messages", "topic", record.topic()).increment();
        log.error("Sending record to dead-letter topic {}{} after retries exhausted. key={} offset={}",
                record.topic(), KafkaTopicsProperties.DLT_SUFFIX, record.key(), record.offset(), exception);
        super.accept(record, consumer, exception);
    }
}
