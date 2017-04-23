package br.com.lucasaugusto.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Created by lucas.augusto on 21/04/17.
 */
@Component
public class Consumer {

    @KafkaListener(topics = "test", id = "consumerTest")
    public void consume(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) final String key, @Payload final String message) {
        System.out.println(key + ": " + message);
    }
}
