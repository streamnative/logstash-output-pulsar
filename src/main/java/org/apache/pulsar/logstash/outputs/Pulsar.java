package org.apache.pulsar.logstash.outputs;

import co.elastic.logstash.api.Codec;
import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.Output;
import co.elastic.logstash.api.PluginConfigSpec;
import co.elastic.logstash.api.PluginHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

    // TLS Config
    private static final String authPluginClassName = "org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls";
    private static final List<String> protocols = Arrays.asList("TLSv1.2");
    private static final List<String> ciphers = Arrays.asList(
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"
    );

    private static final PluginConfigSpec<Boolean> CONFIG_ENABLE_TLS =
            PluginConfigSpec.booleanSetting("enable_tls",false);

    private static final PluginConfigSpec<Boolean> CONFIG_ALLOW_TLS_INSECURE_CONNECTION =
            PluginConfigSpec.booleanSetting("allow_tls_insecure_connection",false);

    private static final PluginConfigSpec<Boolean> CONFIG_ENABLE_TLS_HOSTNAME_VERIFICATION =
            PluginConfigSpec.booleanSetting("enable_tls_hostname_verification",false);

    private static final PluginConfigSpec<String> CONFIG_TLS_TRUST_STORE_PATH =
            PluginConfigSpec.stringSetting("tls_trust_store_path","");

    private static final PluginConfigSpec<String> CONFIG_TLS_TRUST_STORE_PASSWORD =
            PluginConfigSpec.stringSetting("tls_trust_store_password","");

    private static final PluginConfigSpec<String> CONFIG_AUTH_PLUGIN_CLASS_NAME =
            PluginConfigSpec.stringSetting("auth_plugin_class_name",authPluginClassName);

    private static final PluginConfigSpec<List<Object>> CONFIG_CIPHERS =
            PluginConfigSpec.arraySetting("ciphers", Collections.singletonList(ciphers), false, false);

    private static final PluginConfigSpec<List<Object>> CONFIG_PROTOCOLS =
            PluginConfigSpec.arraySetting("protocols", Collections.singletonList(protocols), false, false);

    private static final PluginConfigSpec<Boolean> CONFIG_ENABLE_TOKEN =
            PluginConfigSpec.booleanSetting("enable_token",false);

    private static final PluginConfigSpec<String> CONFIG_AUTH_PLUGIN_PARAMS_STRING =
            PluginConfigSpec.stringSetting("auth_plugin_params_String","");

    private final CountDownLatch done = new CountDownLatch(1);

    private final String producerName;
    private static final PluginConfigSpec<String> CONFIG_PRODUCER_NAME =
            PluginConfigSpec.requiredStringSetting("producer_name");
    private final String id;
    private volatile boolean stopped;
    private final PulsarClient client;
    private final Map<String,org.apache.pulsar.client.api.Producer<byte[]>> producerMap;
    private final String serviceUrl;
    // producer config list
    // can only use java codec
    private final Codec codec;
    // topic
    private final String topic;
    // compressionType LZ4,ZLIB,ZSTD,SNAPPY
    private final String compressionType;
    // blockIfQueueFull true/false
    private final boolean blockIfQueueFull;
    // enableBatching true/false
    private final boolean enableBatching;

    //TLS
    private final boolean enableTls;

    //Token
    private final boolean enableToken;

    // TODO: batchingMaxPublishDelay milliseconds

    // TODO: sendTimeoutMs milliseconds 30000

    // all plugins must provide a constructor that accepts id, Configuration, and Context
    public Pulsar(final String id, final Configuration configuration, final Context context) {
        // constructors should validate configuration options
        this.id = id;
        codec = configuration.get(CONFIG_CODEC);
        if (codec == null) {
            throw new IllegalStateException("Unable to obtain codec");
        }

        serviceUrl = configuration.get(CONFIG_SERVICE_URL);

        topic = configuration.get(CONFIG_TOPIC);
        producerName = configuration.get(CONFIG_PRODUCER_NAME);
        enableBatching = configuration.get(CONFIG_ENABLE_BATCHING);
        blockIfQueueFull = configuration.get(CONFIG_BLOCK_IF_QUEUE_FULL);
        compressionType = configuration.get(CONFIG_COMPRESSION_TYPE);

        enableTls = configuration.get(CONFIG_ENABLE_TLS);
        enableToken = configuration.get(CONFIG_ENABLE_TOKEN);

        try {
            if(enableTls && enableToken){
                logger.error("Unable to Tls and Token authentication at the same time");
                throw new IllegalStateException("Unable to Tls and Token authentication at the same time，enable_tls => true && enable_token => true" );
            } else if (enableTls) {
                // pulsar TLS
                client = buildTlsPulsar(configuration);
            } else if (enableToken) {
                // pulsar Token
                client = buildTokenPulsar(configuration);
            } else {
                client = buildNotTlsPulsar();
            }
            producerMap = new HashMap<>();
        } catch (PulsarClientException e) {
            logger.error("Fail to create pulsar client", e);
            throw new IllegalStateException("Unable to create pulsar client");
        }
    }

    private PulsarClient buildNotTlsPulsar() throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();
    }

    private PulsarClient buildTokenPulsar(Configuration configuration) throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .authentication(configuration.get(CONFIG_AUTH_PLUGIN_CLASS_NAME),configuration.get(CONFIG_AUTH_PLUGIN_PARAMS_STRING))
                .build();
    }

    private PulsarClient buildTlsPulsar(Configuration configuration) throws PulsarClientException {
        Boolean allowTlsInsecureConnection = configuration.get(CONFIG_ALLOW_TLS_INSECURE_CONNECTION);
        Boolean enableTlsHostnameVerification = configuration.get(CONFIG_ENABLE_TLS_HOSTNAME_VERIFICATION);
        String tlsTrustStorePath = configuration.get(CONFIG_TLS_TRUST_STORE_PATH);
        Map<String, String> authMap = new HashMap<>();
        authMap.put(AuthenticationKeyStoreTls.KEYSTORE_TYPE, "JKS");
        authMap.put(AuthenticationKeyStoreTls.KEYSTORE_PATH, tlsTrustStorePath);
        authMap.put(AuthenticationKeyStoreTls.KEYSTORE_PW, configuration.get(CONFIG_TLS_TRUST_STORE_PASSWORD));

        Set<String> cipherSet = new HashSet<>();
        Optional.ofNullable(configuration.get(CONFIG_CIPHERS)).ifPresent(
                cipherList -> cipherList.forEach(cipher -> cipherSet.add(String.valueOf(cipher))));

        Set<String> protocolSet = new HashSet<>();
        Optional.ofNullable(configuration.get(CONFIG_PROTOCOLS)).ifPresent(
                protocolList -> protocolList.forEach(protocol -> protocolSet.add(String.valueOf(protocol))));

        return PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .tlsCiphers(cipherSet)
                .tlsProtocols(protocolSet)
                .allowTlsInsecureConnection(allowTlsInsecureConnection)
                .enableTlsHostnameVerification(enableTlsHostnameVerification)
                .tlsTrustStorePath(tlsTrustStorePath)
                .tlsTrustStorePassword(configuration.get(CONFIG_TLS_TRUST_STORE_PASSWORD))
                .authentication(configuration.get(CONFIG_AUTH_PLUGIN_CLASS_NAME),authMap)
                .build();
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
                        .sendAsync();
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
            ProducerBuilder<byte[]> producerBuilder = client.newProducer()
                    .topic(topic)
                    .enableBatching(enableBatching)
                    .blockIfQueueFull(blockIfQueueFull)
                    .compressionType(getSubscriptionType());
            if (producerName != null) {
                producerBuilder.producerName(producerName);
            }
            org.apache.pulsar.client.api.Producer<byte[]> producer = producerBuilder.create();
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
                CONFIG_PRODUCER_NAME,
                CONFIG_COMPRESSION_TYPE,
                CONFIG_ENABLE_BATCHING,
                CONFIG_BLOCK_IF_QUEUE_FULL,
                CONFIG_AUTH_PLUGIN_CLASS_NAME,

                // Pulsar TLS Config
                CONFIG_ENABLE_TLS,
                CONFIG_TLS_TRUST_STORE_PATH,
                CONFIG_TLS_TRUST_STORE_PASSWORD,
                CONFIG_PROTOCOLS,
                CONFIG_ALLOW_TLS_INSECURE_CONNECTION,
                CONFIG_ENABLE_TLS_HOSTNAME_VERIFICATION,
                CONFIG_CIPHERS,

                // Pulsar Token Config
                CONFIG_ENABLE_TOKEN,
                CONFIG_AUTH_PLUGIN_PARAMS_STRING
        ));

    }

    @Override
    public String getId() {
        return this.id;
    }
}
