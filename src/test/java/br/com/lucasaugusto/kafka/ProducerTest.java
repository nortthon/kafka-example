package br.com.lucasaugusto.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasKey;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

/**
 * Created by lucas.augusto on 23/04/17.
 */
public class ProducerTest {

    private static final String TEMPLATE_TOPIC = "test";

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, TEMPLATE_TOPIC);

    private KafkaTemplate<String, String> kafkaTemplate;

    private BlockingQueue<ConsumerRecord<String, String>> records;

    @Before
    public void setup() throws Exception {
        setupConsumer();
        setupProducer();
    }

    @Test
    public void test() throws Exception {
        final Producer producer = new Producer(kafkaTemplate);
        producer.send("1A2B3C", "New Message");

        final ConsumerRecord<String, String> received = records.poll(10, TimeUnit.SECONDS);
        assertThat(received, hasKey("1A2B3C"));
        assertThat(received, hasValue("New Message"));
    }

    private void setupConsumer() throws Exception {
        final Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("app", "false", embeddedKafka);
        consumerProps.put("key.deserializer", StringDeserializer.class);
        final DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        final ContainerProperties containerProperties = new ContainerProperties(TEMPLATE_TOPIC);
        final KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(cf, containerProperties);

        records = new LinkedBlockingQueue<>();
        container.setupMessageListener((MessageListener<String, String>) records::add);
        container.setBeanName("templateTests");
        container.start();

        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
    }

    private void setupProducer() {
        final Map<String, Object> producerProps = KafkaTestUtils.senderProps(embeddedKafka.getBrokersAsString());
        producerProps.put("key.serializer", StringSerializer.class);
        final DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(pf);
        kafkaTemplate.setDefaultTopic(TEMPLATE_TOPIC);
    }
}
