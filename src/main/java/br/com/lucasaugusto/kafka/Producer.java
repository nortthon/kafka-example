package br.com.lucasaugusto.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.web.bind.annotation.RequestMethod.GET;

/**
 * Created by lucas.augusto on 21/04/17.
 */
@RestController
public class Producer {

    private final  KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public Producer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @RequestMapping(method = GET, value = "/send")
    public void send(@RequestParam("key") final String key, @RequestParam("message") final String message){
        kafkaTemplate.send("test", key, message);
    }
}
