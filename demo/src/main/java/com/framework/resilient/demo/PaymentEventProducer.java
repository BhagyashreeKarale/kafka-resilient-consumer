package com.framework.resilient.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * Produces PaymentEvents to a configurable Kafka topic for demo scenarios.
 * Supports sequenced, out-of-order, duplicate, and poison pill production.
 */
@Component
public class PaymentEventProducer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventProducer.class);

    private final ObjectMapper objectMapper;
    private final String bootstrapServers;
    private final String topic;
    private KafkaProducer<String, byte[]> producer;
    private final Random random = new Random();

    public PaymentEventProducer(
            ObjectMapper objectMapper,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${demo.topic:payment-events}") String topic) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        this.producer = new KafkaProducer<>(props);
        log.info("PaymentEventProducer initialized, topic={}, bootstrap={}", topic, bootstrapServers);
    }

    @PreDestroy
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    /**
     * Produce {@code count} sequenced events for one account in correct order.
     */
    public void produceSequenced(String accountId, int count) {
        log.info("Producing {} sequenced events for account={}", count, accountId);
        for (int i = 1; i <= count; i++) {
            PaymentEvent event = createEvent(accountId, i);
            send(accountId, event);
        }
        producer.flush();
        log.info("Produced {} sequenced events for account={}", count, accountId);
    }

    /**
     * Produce {@code count} events for one account in deliberately shuffled order.
     */
    public void produceOutOfOrder(String accountId, int count) {
        log.info("Producing {} out-of-order events for account={}", count, accountId);
        List<PaymentEvent> events = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            events.add(createEvent(accountId, i));
        }
        Collections.shuffle(events, random);
        for (PaymentEvent event : events) {
            send(accountId, event);
        }
        producer.flush();
        log.info("Produced {} out-of-order events for account={}", count, accountId);
    }

    /**
     * Produce the same event twice to trigger duplicate detection.
     */
    public void produceDuplicates(String accountId, long sequenceNumber) {
        log.info("Producing duplicate events for account={}, seq={}", accountId, sequenceNumber);
        PaymentEvent event = createEvent(accountId, sequenceNumber);
        send(accountId, event);
        send(accountId, event);
        producer.flush();
        log.info("Produced duplicate events for account={}, seq={}", accountId, sequenceNumber);
    }

    /**
     * Produce undeserializable bytes to trigger deserialization error handling.
     */
    public void producePoisonPill() {
        log.info("Producing poison pill event");
        byte[] garbage = "NOT_VALID_JSON{{{garbage!!!".getBytes();
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, "poison", garbage);
        try {
            producer.send(record).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while producing poison pill", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to produce poison pill", e);
        }
        producer.flush();
        log.info("Produced poison pill event");
    }

    private PaymentEvent createEvent(String accountId, long sequenceNumber) {
        String[] currencies = {"USD", "EUR", "GBP"};
        String[] types = {"CREDIT", "DEBIT"};
        return new PaymentEvent(
                accountId,
                sequenceNumber,
                BigDecimal.valueOf(random.nextDouble() * 1000).setScale(2, BigDecimal.ROUND_HALF_UP),
                currencies[random.nextInt(currencies.length)],
                types[random.nextInt(types.length)],
                Instant.now()
        );
    }

    private void send(String key, PaymentEvent event) {
        try {
            byte[] value = objectMapper.writeValueAsBytes(event);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);
            producer.send(record).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while producing event", e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to produce event: " + event, e);
        }
    }
}
