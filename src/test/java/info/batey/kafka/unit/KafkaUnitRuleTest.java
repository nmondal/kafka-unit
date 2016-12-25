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

    public  static void assertKafkaStartsAndSendsMessage(final String testTopic, final KafkaUnit kafkaUnit) throws Exception {
        //given
        kafkaUnit.createTopic(testTopic);
        KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(testTopic, "key", "value");

        //when
        kafkaUnit.sendMessages(keyedMessage);
        List<String> messages = kafkaUnit.readMessages(testTopic, 1);

        //then
        assertEquals(Arrays.asList("value"), messages);

    }

    @Rule
    public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(6000, 6001);

    @Test
    public void junitRuleShouldHaveStartedKafka() throws Exception {
        assertKafkaStartsAndSendsMessage("SpecifiedPort-TestTopic", kafkaUnitRule.getKafkaUnit());
    }

}
