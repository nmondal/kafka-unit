/*
 * Copyright (C) 2014 Christopher Batey
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

import org.junit.Rule;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertEquals;

public class KafkaUnitRuleTest {

    @Rule
    public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(6000, 6001);

    @Test
    public void produceMapAsMessage() throws Exception{
        // Get Unit
        KafkaUnit kafkaUnit = kafkaUnitRule.getKafkaUnit();
        final String testTopic = "maps-as-messages-test" ;
        //given
        kafkaUnit.createTopic(testTopic);
        // list of message from map
        List l = Arrays.asList( KafkaUnit.createRecordMap( testTopic, "key", "value" ) );
        // send one
        kafkaUnit.sendMessages(l);
        // get back
        List<String> messages = kafkaUnit.readMessages( testTopic );
        assertEquals( 1 , messages.size() );
        assertEquals( "value", messages.get(0) );
    }


    @Test
    public void sendAndReceiveMultipleMessages() throws Exception {
        final String testTopic = "send-receive-multiple-message-test" ;
        // Get Unit
        KafkaUnit kafkaUnit = kafkaUnitRule.getKafkaUnit();
        //given
        kafkaUnit.createTopic(testTopic);
        // send
        kafkaUnit.sendMessages(testTopic, "value1", "value2");
        // receive ...
        List<String> received = kafkaUnit.readMessages( testTopic );
        assertEquals( 2, received.size());
        assertEquals( received.get(0) , "value1" );
        assertEquals( received.get(1) , "value2" );
    }
}
