/*
 * Copyright (C) 2016 Nabarun Mondal
 *
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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by noga on 25/12/16.
 */
public class ConnectionStringKafkaUnitRuleTest {

    @Rule
    public KafkaUnitRule kafkaUnitRuleWithConnectionStrings = new KafkaUnitRule("localhost:5000", "localhost:5001");

    @Test
    public void producerConsumerFromRuleShouldBeFine() throws Exception {
        // Get Unit
        KafkaUnit kafkaUnit = kafkaUnitRuleWithConnectionStrings.getKafkaUnit();
        final String testTopic = "producer-consumer-test" ;
        //given
        kafkaUnit.createTopic(testTopic);
        // get producer
        KafkaProducer producer = kafkaUnit.producer();
        // create record
        ProducerRecord<String, String> record = new ProducerRecord<>(testTopic, "key", "value");
        // send record
        producer.send( record );
        // get consumer ...
        KafkaConsumer consumer = kafkaUnit.consumer();
        // subscribe to topics
        consumer.subscribe( Arrays.asList( testTopic) );
        // get records
        ConsumerRecords consumerRecords = consumer.poll(6000);
        // test if the count is 1
        assertEquals(1, consumerRecords.count() );
        Iterable<ConsumerRecord<String,String>> records = consumerRecords.records(testTopic);
        for (ConsumerRecord<String,String> r : records ){
            assertEquals(record.topic(), r.topic() );
            assertEquals( record.key(), r.key() );
            assertEquals( record.value(), r.value() );
        }
    }

    @Test
    public void consumeArbitraryMessages() throws Exception{
        // Get Unit
        KafkaUnit kafkaUnit = kafkaUnitRuleWithConnectionStrings.getKafkaUnit();
        final String testTopic = "consumer-of-arbitrary-no-of-messages-test" ;
        //given
        kafkaUnit.createTopic(testTopic);
        // create record
        ProducerRecord<String, String> record = new ProducerRecord<>(testTopic, "key", "value");
        // send record
        kafkaUnit.sendMessages( record );
        // read back ...
        List<String> messages = kafkaUnit.readMessages( testTopic );
        assertEquals( 1 , messages.size() );
        assertEquals( record.value(), messages.get(0) );
    }

}
