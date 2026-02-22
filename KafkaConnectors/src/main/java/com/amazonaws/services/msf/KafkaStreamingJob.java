package com.amazonaws.services.msf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.amazonaws.services.msf.domain.StockPrice;
import com.amazonaws.services.msf.jdbc.StockPriceDB;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import static com.amazonaws.services.msf.ConfigurationHelper.*;


public class KafkaStreamingJob {

    private static final String DEFAULT_GROUP_ID = "my-group";
    private static final String DEFAULT_SOURCE_TOPIC = "source";
    private static final String DEFAULT_SINK_TOPIC = "destination";
    private static final int DEFAULT_RECORDS_PER_SECOND = 10;
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final long DEFAULT_BATCH_INTERVAL_MS = 200L;
    private static final int DEFAULT_MAX_RETRIES = 5;
    private static final OffsetsInitializer DEFAULT_OFFSETS_INITIALIZER = OffsetsInitializer.latest();

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamingJob.class);

    private static final String LOCAL_APPLICATION_PROPERTIES_RESOURCE = "application_properties.json";


    private static boolean isLocal(StreamExecutionEnvironment env) {
        // return env instanceof LocalStreamEnvironment;
        return true;
    }


    /**
     * Load application properties from Amazon Managed Service for Apache Flink runtime or from a local resource, when the environment is local
     */
    private static Map<String, Properties> loadApplicationProperties(StreamExecutionEnvironment env) throws IOException {
        if (isLocal(env)) {
            LOG.info("Loading application properties from '{}'", LOCAL_APPLICATION_PROPERTIES_RESOURCE);
            Map<String, Properties> propMap = KinesisAnalyticsRuntime.getApplicationProperties(
                    KafkaStreamingJob.class.getClassLoader().getResource(LOCAL_APPLICATION_PROPERTIES_RESOURCE).getPath()
            );
            if (propMap.isEmpty()) {
                // Fallback: load manually from resources for local run
                InputStream in = KafkaStreamingJob.class.getClassLoader().getResourceAsStream(LOCAL_APPLICATION_PROPERTIES_RESOURCE);
                if (in == null) throw new FileNotFoundException("application_properties.json not found in resources.");
                // ObjectMapper mapper = new ObjectMapper();
                // List<Map<String, Object>> groups = mapper.readValue(in, List.class);
                ObjectMapper mapper = new ObjectMapper();
                List<Map<String, Object>> groups = mapper.readValue(
                    in, new TypeReference<List<Map<String, Object>>>() {}
                );

                Map<String, Properties> localProps = new HashMap<>();
                for (Map<String, Object> group : groups) {
                    String groupId = (String) group.get("PropertyGroupId");
                    Map<String, String> propertyMap = (Map<String, String>) group.get("PropertyMap");
                    Properties props = new Properties();
                    props.putAll(propertyMap);
                    localProps.put(groupId, props);
                }
                return localProps;
            }
            return propMap;
        } else {
            LOG.info("Loading application properties from Amazon Managed Service for Apache Flink");
            return KinesisAnalyticsRuntime.getApplicationProperties();
        }
    }


    private static <T> KafkaSource<T> createKafkaSource(Properties inputProperties, OffsetsInitializer offset, final DeserializationSchema<T> valueDeserializationSchema) {
        return KafkaSource.<T>builder()
                .setBootstrapServers(inputProperties.getProperty("bootstrap.servers"))
                .setTopics(inputProperties.getProperty("topic", DEFAULT_SOURCE_TOPIC))
                .setGroupId(inputProperties.getProperty("group.id", DEFAULT_GROUP_ID))
                .setStartingOffsets(offset) // Used when the application starts with no state
                .setValueOnlyDeserializer(valueDeserializationSchema)
                .setProperties(inputProperties)
                .build();
    }


    private static Properties mergeProperties(Properties properties, Properties authProperties) {
        properties.putAll(authProperties);
        return properties;
    }


    private static JdbcSink<StockPrice> createUpsertJdbcSink(Properties sinkProperties) {
        Preconditions.checkNotNull(sinkProperties, "JdbcSink configuration group missing");

        // This example is designed for PostgreSQL. Switching to a different RDBMS requires modifying
        // StockPriceUpsertQueryStatement implementation which depends on the upsert syntax of the specific RDBMS.
        String jdbcDriver = "org.postgresql.Driver";

        String jdbcUrl = extractRequiredStringParameter(sinkProperties, "url", "JDBC URL is required");
        String dbUser = extractRequiredStringParameter(sinkProperties, "username", "JDBC username is required");
        // In the real application the password should have been encrypted or fetched at runtime
        String dbPassword = extractRequiredStringParameter(sinkProperties, "password", "JDBC password is required");

        String tableName = extractStringParameter(sinkProperties, "table.name", "prices");

        int batchSize = extractIntParameter(sinkProperties, "batch.size", DEFAULT_BATCH_SIZE);
        long batchIntervalMs = extractLongParameter(sinkProperties, "batch.interval.ms", DEFAULT_BATCH_INTERVAL_MS);
        int maxRetries = extractIntParameter(sinkProperties, "max.retries", DEFAULT_MAX_RETRIES);

        LOG.info("JDBC Sink configuration - batchSize: {}, batchIntervalMs: {}, maxRetries: {}",
                batchSize, batchIntervalMs, maxRetries);

        return JdbcSink.<StockPrice>builder()
                // The JdbcQueryStatement implementation provides the SQL statement template and converts the input record
                // into parameters passed to the statement.
                .withQueryStatement(new StockPriceDB(tableName))
                .withExecutionOptions(JdbcExecutionOptions.builder()
                        .withBatchSize(batchSize)
                        .withBatchIntervalMs(batchIntervalMs)
                        .withMaxRetries(maxRetries)
                        .build())
                // The SimpleJdbcConnectionProvider is good enough in this case. The connector will open one db connection per parallelism
                // and reuse the same connection on every write. There is no need of a connection pooler
                .buildAtLeastOnce(new SimpleJdbcConnectionProvider(new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcUrl)
                        .withDriverName(jdbcDriver)
                        .withUsername(dbUser)
                        .withPassword(dbPassword)
                        .build())
                );
    }


    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        if( isLocal(env)) {
            env.enableCheckpointing(10_000);
            env.setParallelism(3);
        }

        // Load the application properties
        final Map<String, Properties> applicationProperties = loadApplicationProperties(env);

        LOG.info("Application properties: {}", applicationProperties);

        // Get the AuthProperties if present (only relevant when using IAM Auth)
        Properties authProperties = applicationProperties.getOrDefault("AuthProperties", new Properties());

        // Prepare the Source and Sink properties
        Properties inputProperties = mergeProperties(applicationProperties.get("DockerKafka"), authProperties);
        // Properties inputProperties = mergeProperties(applicationProperties.get("localKafka"), authProperties);
        // Properties outputProperties = mergeProperties(applicationProperties.get("OutputKafka0"), authProperties);

        // Create and add the Source
        OffsetsInitializer offsetKafka = isLocal(env) ? OffsetsInitializer.earliest() : DEFAULT_OFFSETS_INITIALIZER;
        KafkaSource<StockPrice> source = createKafkaSource(inputProperties, offsetKafka, new JsonDeserializationSchema<>(StockPrice.class));
        DataStream<StockPrice> stockStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");

        // KafkaRecordSerializationSchema<StockPrice> recordSerializationSchema = KafkaRecordSerializationSchema.<StockPrice>builder()
        //         .setTopic(outputProperties.getProperty("topic", DEFAULT_SINK_TOPIC))
        //         // Use a field as kafka record key
        //         // Define no keySerializationSchema to publish kafka records with no key
        //         .setKeySerializationSchema(stock -> (
        //             stock.getSymbol() + "|" +
        //             stock.getWindowStart() + "|" +
        //             stock.getWindowEnd()).getBytes())
        //         // Serialize the Kafka record value (payload) as JSON
        //         .setValueSerializationSchema(new JsonSerializationSchema<>())
        //         .build();
                    
        
        // Create the JDBC sink
        // Properties sinkProperties = applicationProperties.get("LocalJdbcPostgresSink");
        // Properties sinkProperties = applicationProperties.get("LocalJdbcTimescaleDBSink");
        Properties sinkProperties = applicationProperties.get("DockerJdbcTimescaleDBSink");
        JdbcSink<StockPrice> jdbcSink = createUpsertJdbcSink(sinkProperties);
        // Attach the sink
        stockStream.sinkTo(jdbcSink).uid("jdbc-sink").name("PostgreSQL Sink");

        if (isLocal(env)) {
            stockStream.print("<< DEBUG ALERT_1_MINSTREAM >>");
        }
        // Create and add the Sink
        // KafkaSink<StockPrice> sink = createKafkaSink(outputProperties, recordSerializationSchema);
        // input.sinkTo(sink);

        env.execute("Flink Kafka Source and Sink examples");
    }
}
