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

import org.junit.Rule;
import org.junit.Test;

/**
 * Created by noga on 25/12/16.
 */
public class KafkaUnitRuleWithConnectionStringTest  {

    @Rule
    public KafkaUnitRule kafkaUnitRuleWithConnectionStrings = new KafkaUnitRule("localhost:5000", "localhost:5001");

    @Test
    public void junitRuleShouldHaveStartedKafkaWithConnectionStrings() throws Exception {
        KafkaUnitRuleTest.assertKafkaStartsAndSendsMessage("ConnectionString-TestTopic",
                kafkaUnitRuleWithConnectionStrings.getKafkaUnit());
    }
}
