package org.apache.pulsar.logstash.outputs;

import co.elastic.logstash.api.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.*;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.CountDownLatch;

@LogstashPlugin(name = "pulsar")
public class Pulsar implements Output {

    public static final PluginConfigSpec<Codec> CONFIG_CODEC =
            PluginConfigSpec.codecSetting("codec", "java_line");

    private final static Logger logger = LogManager.getLogger(Pulsar.class);

    private static final PluginConfigSpec<String> CONFIG_SERVICE_URL =
            PluginConfigSpec.stringSetting("serviceUrl", "pulsar://localhost:6650");

    private static final PluginConfigSpec<String> CONFIG_TOPIC =
            PluginConfigSpec.requiredStringSetting("topic");

    private static final String COMPRESSION_TYPE_NONE = "NONE";
    private static final String COMPRESSION_TYPE_LZ4 = "LZ4";
    private static final String COMPRESSION_TYPE_ZLIB = "ZLIB";
    private static final String COMPRESSION_TYPE_ZSTD = "ZSTD";
    private static final String COMPRESSION_TYPE_SNAPPY = "SNAPPY";
    private static final PluginConfigSpec<String> CONFIG_COMPRESSION_TYPE =
            PluginConfigSpec.stringSetting("compression_type", COMPRESSION_TYPE_NONE);

    private static final PluginConfigSpec<Boolean> CONFIG_BLOCK_IF_QUEUE_FULL =
            PluginConfigSpec.booleanSetting("block_if_queue_full",true);

    private static final PluginConfigSpec<Boolean> CONFIG_ENABLE_BATCHING =
            PluginConfigSpec.booleanSetting("enable_batching",true);

    private final CountDownLatch done = new CountDownLatch(1);

    // producer_name
//    private String producerName;
//    private static final PluginConfigSpec<String> CONFIG_PRODUCER_NAME =
//            PluginConfigSpec.requiredStringSetting("producer_name");
    private String id;
    private volatile boolean stopped;
    private OutputStream outputStream;
    private PulsarClient client;
    private Map<String,org.apache.pulsar.client.api.Producer<byte[]>> producerMap;
    private String serviceUrl;
    // producer config list
    // can only use java codec
    private Codec codec;
    // topic
    private String topic;
    // compressionType LZ4,ZLIB,ZSTD,SNAPPY
    private String compressionType;
    // blockIfQueueFull true/false
    private boolean blockIfQueueFull;
    // enableBatching true/false
    private boolean enableBatching;

    // TODO: batchingMaxPublishDelay milliseconds

    // TODO: sendTimeoutMs milliseconds 30000
    private PrintStream printer;

    // all plugins must provide a constructor that accepts id, Configuration, and Context
    public Pulsar(final String id, final Configuration configuration, final Context context) {
        this(id, configuration, context, System.out);
    }

    Pulsar(final String id, final Configuration config, final Context context, OutputStream targetStream) {
        // constructors should validate configuration options
        this.id = id;
        this.outputStream = targetStream;
        codec = config.get(CONFIG_CODEC);
        if (codec == null) {
            throw new IllegalStateException("Unable to obtain codec");
        }

        serviceUrl = config.get(CONFIG_SERVICE_URL);

        topic = config.get(CONFIG_TOPIC);
//        producerName = config.get(CONFIG_PRODUCER_NAME);
        enableBatching = config.get(CONFIG_ENABLE_BATCHING);
        blockIfQueueFull = config.get(CONFIG_BLOCK_IF_QUEUE_FULL);
        compressionType = config.get(CONFIG_COMPRESSION_TYPE);

        try {

            client = PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    .build();
            producerMap = new HashMap<>();
        } catch (PulsarClientException e) {
            logger.error("fail to create pulsar client", e);
            throw new IllegalStateException("Unable to create pulsar client");
        }
    }

    @Override
    public void output(final Collection<Event> events) {
        Iterator<Event> z = events.iterator();
        while (z.hasNext() && !stopped) {
            try {
                Event event = z.next();
                String eventTopic = event.sprintf(topic);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                codec.encode(event, baos);
                String s = baos.toString();
                logger.debug("topic is {}, message is {}", eventTopic, s);
                getProducer(eventTopic).newMessage()
                        .value(s.getBytes())
                        .send();
            } catch (Exception e) {
                logger.error("fail to send message", e);
            }
        }
    }

    private org.apache.pulsar.client.api.Producer<byte[]> getProducer(String topic) throws PulsarClientException {

        if(producerMap.containsKey(topic)){
            return producerMap.get(topic);
        }else{
            // Create a producer
            org.apache.pulsar.client.api.Producer<byte[]> producer = client.newProducer()
                    .topic(topic)
                    .enableBatching(enableBatching)
//                    .producerName(producerName)
                    .blockIfQueueFull(blockIfQueueFull)
                    .compressionType(getSubscriptionType())
                    .create();
            logger.info("Create producer {} to topic {} , blockIfQueueFull is {},compressionType is {}", producer.getProducerName(),topic, blockIfQueueFull?"true":"false",compressionType);
            producerMap.put(topic,producer);
            return producer;
        }
    }

    private CompressionType getSubscriptionType() {
        CompressionType type = CompressionType.NONE;
        switch (compressionType) {
            case COMPRESSION_TYPE_LZ4:
                type = CompressionType.LZ4;
                break;
            case COMPRESSION_TYPE_ZLIB:
                type = CompressionType.ZLIB;
                break;
            case COMPRESSION_TYPE_ZSTD:
                type = CompressionType.ZSTD;
                break;
            case COMPRESSION_TYPE_SNAPPY:
                type = CompressionType.SNAPPY;
                break;
            default:
                logger.warn("{} is not one known compression type! 'NONE' type will be used! ", compressionType);
        }
        return type;
    }

    private void closePulsarProducer() {
        try {
            for(org.apache.pulsar.client.api.Producer<byte[]> producer: producerMap.values()){
                producer.close();
                logger.info("close producer {} for topic {}",producer.getProducerName(),producer.getTopic());
            }
            client.close();
            logger.info("close pulsar client");
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void stop() {
        stopped = true; // set flag to request cooperative stop of input
        closePulsarProducer();
        done.countDown();
    }

    @Override
    public void awaitStop() throws InterruptedException {
        done.await(); // blocks until input has stopped
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        // should return a list of all configuration options for this plugin
        return PluginHelper.commonOutputSettings(Arrays.asList(
                CONFIG_CODEC,
                CONFIG_SERVICE_URL,
                CONFIG_TOPIC,
//                CONFIG_PRODUCER_NAME,
                CONFIG_COMPRESSION_TYPE,
                CONFIG_ENABLE_BATCHING,
                CONFIG_BLOCK_IF_QUEUE_FULL
        ));

    }

    @Override
    public String getId() {
        return this.id;
    }


}
