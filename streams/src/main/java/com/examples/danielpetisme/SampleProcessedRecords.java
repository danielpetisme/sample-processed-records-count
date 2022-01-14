package com.examples.danielpetisme;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Hello world!
 */
public class SampleProcessedRecords {

    private static final String KAFKA_ENV_PREFIX = "KAFKA_";
    private final Logger logger = LoggerFactory.getLogger(SampleProcessedRecords.class);
    private final KafkaStreams streams;
    private final Topics topics;

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        new SampleProcessedRecords(Collections.emptyMap()).start();
    }

    public SampleProcessedRecords(Map<String, String> config) throws InterruptedException, ExecutionException {
        var properties = buildProperties(defaultProps, System.getenv(), KAFKA_ENV_PREFIX, config);
        AdminClient adminClient = KafkaAdminClient.create(properties);
        this.topics = new Topics(adminClient);

        var topology = buildTopology();
        logger.info(topology.describe().toString());
        logger.info("creating streams with props: {}", properties);
        streams = new KafkaStreams(topology, properties);
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        addPrometheusEndpoint();
    }

    private void addPrometheusEndpoint() {
        int port = Integer.parseInt(System.getenv().getOrDefault("PROMETHEUS_PORT", "8080"));
        PrometheusMeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        new KafkaStreamsMetrics(this.streams).bindTo(prometheusRegistry);

        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/metrics", httpExchange -> {
                String response = prometheusRegistry.scrape();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });

            new Thread(server::start).start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    Topology buildTopology() {

        var builder = new StreamsBuilder();
        var input = builder.stream(
                topics.inputTopic,
                Consumed.with(Serdes.String(), Serdes.String()));

        input
                .mapValues(v -> v + "_mapped")
                .repartition(Repartitioned.as("my-repartition"))
                .mapValues(v -> v + "_repartitioned")
                .to(topics.outputTopic);

        return builder.build();

    }

    public void start() {
        logger.info("Kafka Streams started");
        streams.start();
    }

    public void stop() {
        streams.close(Duration.ofSeconds(3));
        logger.info("Kafka Streams stopped");
    }

    private Map<String, String> defaultProps = Map.of(
            StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.TRACE.name,
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:49881",
            StreamsConfig.APPLICATION_ID_CONFIG, System.getenv().getOrDefault("APPLICATION_ID", SampleProcessedRecords.class.getName()),
            StreamsConfig.REPLICATION_FACTOR_CONFIG, "1",
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName(),
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName(),
            StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest"
    );

    private Properties buildProperties(Map<String, String> baseProps, Map<String, String> envProps, String prefix, Map<String, String> customProps) {
        Map<String, String> systemProperties = envProps.entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(
                        e -> e.getKey()
                                .replace(prefix, "")
                                .toLowerCase()
                                .replace("_", ".")
                        , e -> e.getValue())
                );

        Properties props = new Properties();
        props.putAll(baseProps);
        props.putAll(systemProperties);
        props.putAll(customProps);
        return props;
    }
}
