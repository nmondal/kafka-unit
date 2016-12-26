/*
 * Copyright (C) 2014 Christopher Batey
 * Copyright (C) 2016 Nabarun Mondal
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.batey.kafka.unit;

import kafka.admin.TopicCommand;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.VerifiableProperties;
import kafka.utils.ZkUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ComparisonFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;

public class KafkaUnit {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUnit.class);

    private KafkaServerStartable broker;

    private Zookeeper zookeeper;
    private final String zookeeperString;
    private final String brokerString;
    private int zkPort;
    private int brokerPort;
    private KafkaProducer<String, String> producer = null;
    private Properties kafkaBrokerConfig = new Properties();

    public KafkaUnit() throws IOException {
        this(getEphemeralPort(), getEphemeralPort());
    }

    public KafkaUnit(int zkPort, int brokerPort) {
        this.zkPort = zkPort;
        this.brokerPort = brokerPort;
        this.zookeeperString = "localhost:" + zkPort;
        this.brokerString = "localhost:" + brokerPort;
    }

    public KafkaUnit(String zkConnectionString, String kafkaConnectionString) {
        this(parseConnectionString(zkConnectionString), parseConnectionString(kafkaConnectionString));
    }

    private static int parseConnectionString(String connectionString) {
        try {
            String[] hostPorts = connectionString.split(",");

            if (hostPorts.length != 1) {
                throw new IllegalArgumentException("Only one 'host:port' pair is allowed in connection string");
            }

            String[] hostPort = hostPorts[0].split(":");

            if (hostPort.length != 2) {
                throw new IllegalArgumentException("Invalid format of a 'host:port' pair");
            }

            if (!"localhost".equals(hostPort[0])) {
                throw new IllegalArgumentException("Only localhost is allowed for KafkaUnit");
            }

            return Integer.parseInt(hostPort[1]);
        } catch (Exception e) {
            throw new RuntimeException("Cannot parse connectionString " + connectionString, e);
        }
    }

    private static int getEphemeralPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    public void startup() {
        zookeeper = new Zookeeper(zkPort);
        zookeeper.startup();

        final File logDir;
        try {
            logDir = Files.createTempDirectory("kafka").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start Kafka", e);
        }
        logDir.deleteOnExit();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    FileUtils.deleteDirectory(logDir);
                } catch (IOException e) {
                    LOGGER.warn("Problems deleting temporary directory " + logDir.getAbsolutePath(), e);
                }
            }
        }));
        kafkaBrokerConfig.setProperty("zookeeper.connect", zookeeperString);
        kafkaBrokerConfig.setProperty("broker.id", "1");
        kafkaBrokerConfig.setProperty("host.name", "localhost");
        kafkaBrokerConfig.setProperty("port", Integer.toString(brokerPort));
        kafkaBrokerConfig.setProperty("log.dir", logDir.getAbsolutePath());
        kafkaBrokerConfig.setProperty("log.flush.interval.messages", String.valueOf(1));

        broker = new KafkaServerStartable(new KafkaConfig(kafkaBrokerConfig));
        broker.startup();
    }

    public String getKafkaConnect() {
        return brokerString;
    }

    public int getZkPort() {
        return zkPort;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public boolean createTopic(String topicName) {
        return createTopic(topicName, 1);
    }

    /**
     * Tries to create a topic
     *
     * @param topicName     name of the topic
     * @param numPartitions number of partitions
     * @return true if topic creation successful, false if topic already exists
     */
    public boolean createTopic(String topicName, Integer numPartitions) {
        // setup
        String[] arguments = new String[9];
        arguments[0] = "--create";
        arguments[1] = "--zookeeper";
        arguments[2] = zookeeperString;
        arguments[3] = "--replication-factor";
        arguments[4] = "1";
        arguments[5] = "--partitions";
        arguments[6] = "" + Integer.valueOf(numPartitions);
        arguments[7] = "--topic";
        arguments[8] = topicName;
        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);

        ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
                30000, 30000, JaasUtils.isZkSecurityEnabled());

        // run
        LOGGER.info("Executing: CreateTopic " + Arrays.toString(arguments));
        try {
            TopicCommand.createTopic(zkUtils, opts);
            return true;
        } catch (TopicExistsException te) {
            LOGGER.error("Topic : " + topicName + " , Already Exists!");
            return false;
        }
    }


    public void shutdown() {
        if (broker != null) broker.shutdown();
        if (zookeeper != null) zookeeper.shutdown();
    }

    public List<KeyedMessage<String, String>> readKeyedMessages(final String topicName, final Integer expectedMessages) throws TimeoutException {
        return readMessages(topicName, expectedMessages, new MessageExtractor<KeyedMessage<String, String>>() {

            @Override
            public KeyedMessage<String, String> extract(MessageAndMetadata<String, String> messageAndMetadata) {
                return new KeyedMessage(topicName, messageAndMetadata.key(), messageAndMetadata.message());
            }
        });
    }

    public List<String> readMessages(String topicName, final Integer expectedMessages) throws TimeoutException {
        return readMessages(topicName, expectedMessages, new MessageExtractor<String>() {
            @Override
            public String extract(MessageAndMetadata<String, String> messageAndMetadata) {
                return messageAndMetadata.message();
            }
        });
    }

    public List<String> readMessages(String topicName) throws TimeoutException {
        return readMessages(topicName, null, new MessageExtractor<String>() {
            @Override
            public String extract(MessageAndMetadata<String, String> messageAndMetadata) {
                return messageAndMetadata.message();
            }
        });
    }

    private <T> List<T> readMessages(String topicName, final Integer expectedMessages, final MessageExtractor<T> messageExtractor) throws TimeoutException {
        ExecutorService singleThread = Executors.newSingleThreadExecutor();
        Properties consumerProperties = new Properties();
        consumerProperties.put("zookeeper.connect", zookeeperString);
        consumerProperties.put("group.id", "10");
        consumerProperties.put("socket.timeout.ms", "500");
        consumerProperties.put("consumer.id", "test");
        consumerProperties.put("auto.offset.reset", "smallest");
        consumerProperties.put("consumer.timeout.ms", "500");
        ConsumerConnector javaConsumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));
        StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties(new Properties()));
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(topicName, 1);
        Map<String, List<KafkaStream<String, String>>> events = javaConsumerConnector.createMessageStreams(topicMap, stringDecoder, stringDecoder);
        List<KafkaStream<String, String>> events1 = events.get(topicName);
        final KafkaStream<String, String> kafkaStreams = events1.get(0);


        Future<List<T>> submit = singleThread.submit(new Callable<List<T>>() {
            public List<T> call() throws Exception {
                List<T> messages = new ArrayList<>();
                try {
                    for (MessageAndMetadata<String, String> kafkaStream : kafkaStreams) {
                        T message = messageExtractor.extract(kafkaStream);
                        LOGGER.info("Received message: {}", kafkaStream.message());
                        messages.add(message);
                    }
                } catch (ConsumerTimeoutException e) {
                    // always gets throws reaching the end of the stream
                }
                // only when it is non null, makes sense --> sometimes we do not know , right?
                if (expectedMessages != null && messages.size() != expectedMessages) {
                    throw new ComparisonFailure("Incorrect number of messages returned", Integer.toString(expectedMessages),
                            Integer.toString(messages.size()));
                }
                return messages;
            }
        });

        try {
            return submit.get(3, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (e.getCause() instanceof ComparisonFailure) {
                throw (ComparisonFailure) e.getCause();
            }
            throw new TimeoutException("Timed out waiting for messages");
        } finally {
            singleThread.shutdown();
            javaConsumerConnector.shutdown();
        }
    }

    private synchronized KafkaProducer<String, String> getKafkaProducer(Class keyClass, Class valueClass) {
        if (producer == null) {
            Properties props = new Properties();
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keyClass.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueClass.getName());

            props.put("metadata.broker.list", brokerString);
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaConnect());
            producer = new KafkaProducer<>(props);
        }
        return producer;
    }


    public static ProducerRecord producerRecord(Map map) {
        String topic = (String) map.get("t");
        Integer partition = (Integer) map.get("p");
        Long timestamp = (Long) map.get("ts");
        Object key = map.get("k");
        Object value = map.get("v");

        return new ProducerRecord(topic, partition, timestamp, key, value);
    }

    public static Map createRecordMap(String topic, String key, String value, Integer partition, Long timeStamp) {
        Map m = new HashMap();
        m.put("t", topic);
        m.put("k", key);
        m.put("v", value);
        if (partition != null) {
            m.put("p", partition);
        }
        if (timeStamp != null) {
            m.put("ts", timeStamp);
        }
        return m;
    }

    public static Map createRecordMap(String topic, String key, String value) {
        return createRecordMap( topic, key, value, null, null);
    }


    public final void sendMessages(List<Map> messages) {
        if (null == messages) return;
        for (int i = 0; i < messages.size(); i++) {
            ProducerRecord pr = producerRecord(messages.get(i));
            sendMessages(pr);
        }
    }

    @SafeVarargs
    public final void sendMessages(KeyedMessage<String, String>... messages) {
        if ( messages == null ) return;

        ProducerRecord<String, String>[] ms = new ProducerRecord[messages.length];
        int i = 0;
        for (KeyedMessage km : messages) {
            ms[i++] = km.record;
        }
        sendMessages(ms);
    }

    @SafeVarargs
    public final void sendMessages(ProducerRecord<String, String>... messages) {
        if ( messages == null ) return;

        producer = getKafkaProducer(StringSerializer.class, StringSerializer.class);
        for (ProducerRecord r : messages) {
            producer.send(r);
        }
    }


    @SafeVarargs
    public final void sendMessages(final String topic, final String... messages) {
        if ( messages == null ) return;
        producer = getKafkaProducer(StringSerializer.class, StringSerializer.class);
        for (String m : messages) {
            producer.send( new ProducerRecord<String, String>( topic, m ) );
        }
    }

    /**
     * Set custom broker configuration.
     * See available config keys in the kafka documentation: http://kafka.apache.org/documentation.html#brokerconfigs
     *
     * @param configKey   the kafka key to be set
     * @param configValue the value for the key
     */
    public final void setKafkaBrokerConfig(String configKey, String configValue) {
        kafkaBrokerConfig.setProperty(configKey, configValue);
    }


    public Properties producerDefault() {
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaConnect());
        return props;
    }

    public KafkaProducer<String, String> producer(Properties props) {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaConnect());
        return new KafkaProducer<>(props);
    }

    public KafkaProducer<String, String> producer() {
        return producer(producerDefault());
    }


    public Properties consumerDefault() {
        Properties props = new Properties();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getCanonicalName());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getCanonicalName());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaConnect());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, "group-" + System.nanoTime());
        props.put("zookeeper.connect", zookeeperString);
        props.put("socket.timeout.ms", "5000");
        props.put("consumer.id", "kafka-unit-test" + String.valueOf(System.nanoTime()));
        props.put("auto.offset.reset", "earliest");
        props.put("consumer.timeout.ms", "5000");

        return props;
    }

    public KafkaConsumer<String, String> consumer(Properties props) {
        // change only which are necessary
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaConnect());
        props.put("zookeeper.connect", zookeeperString);
        return new KafkaConsumer<>(props);
    }

    public KafkaConsumer<String, String> consumer() {
        return consumer(consumerDefault());
    }

    private interface MessageExtractor<T> {
        T extract(MessageAndMetadata<String, String> messageAndMetadata);
    }
}

