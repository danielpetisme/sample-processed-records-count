package com.examples.danielpetisme;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class SampleProcessedRecordsTest {

    KafkaProducer<String, String> inputProducer;
    KafkaConsumer<String, String> inputConsumer;
    KafkaConsumer<String, String> outputConsumer;
    Topics topics;
    SampleProcessedRecords streams;

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.1"));

    @Before
    public void beforeEach() throws ExecutionException, InterruptedException {
        topics = new Topics(AdminClient.create(Map.of(
                BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
        )));

        inputProducer = new KafkaProducer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE
                ),
                new StringSerializer(), new StringSerializer()
        );

        inputConsumer = new KafkaConsumer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "inputConsumer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(), new StringDeserializer()
        );
        inputConsumer.subscribe(Collections.singletonList(topics.inputTopic));

        outputConsumer = new KafkaConsumer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "outputConsumer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(), new StringDeserializer()
        );

        outputConsumer.subscribe(Collections.singletonList(topics.outputTopic));
    }

    @After
    public void afterEach() {
        streams.stop();
    }

    private void loadInput(List<KeyValue<String, String>> input) {
        input.forEach(it -> {
            System.out.println("Producing key=" + it.key + ", Value= " + it.value);
            try {
                inputProducer.send(
                        new ProducerRecord<>(topics.inputTopic, it.key, it.value),
                        (RecordMetadata metadata, Exception exception) -> {
                            if (exception != null) {
                                fail(exception.getMessage());
                            }
                            System.out.println("Produced " + metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());
                        }
                ).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        List<KeyValue<String, String>> loaded = new ArrayList<>();

        long start = System.currentTimeMillis();
        while (loaded.isEmpty() && System.currentTimeMillis() - start < 20_000) {
            ConsumerRecords<String, String> records = inputConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            records.forEach((record) -> loaded.add(new KeyValue<>(record.key(), record.value())));
        }
        assertThat(loaded).hasSize(input.size());
        System.out.println("## Input");
        loaded.forEach(it -> {
            System.out.println(it.key + ", " + it.value);
        });
    }

    @Test
    public void testProcessedRecordsCount() throws Exception {
        streams = new SampleProcessedRecords(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
                )
        );
        streams.start();

//        loadInput(List.of(
//                new KeyValue<>("1", "101")
//        ));

        List<KeyValue<String, String>> loaded = new ArrayList<>();

//        long start = System.currentTimeMillis();
//        while (loaded.isEmpty() && System.currentTimeMillis() - start < 20_000) {
//            ConsumerRecords<String, String> records = outputConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
//            records.forEach((record) -> loaded.add(new KeyValue<>(record.key(), record.value())));
//        }

        assertThat(loaded).isNotEmpty();
    }

}