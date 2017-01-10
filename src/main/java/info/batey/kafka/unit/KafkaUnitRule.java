/*
 * Copyright (C) 2014 Christopher Batey
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

import org.junit.rules.ExternalResource;

import java.io.IOException;

/**
 * JUnit Rule Style for Kafka
 */
public class KafkaUnitRule extends ExternalResource {

    private final KafkaUnit kafkaUnit;

    /**
     * Ephemeral style creation
     */
    public KafkaUnitRule() {
        try {
            this.kafkaUnit = new KafkaUnit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creation using zookeeper port and kafka port
     * @param zkPort the zookeeper port
     * @param kafkaPort kafka port
     */
    public KafkaUnitRule(int zkPort, int kafkaPort) {
        this.kafkaUnit = new KafkaUnit(zkPort, kafkaPort);
    }

    /**
     * Creating using connection strings
     * @param zkConnectionString zookeeper connection string
     * @param kafkaConnectionString kafka connection string
     */
    public KafkaUnitRule(String zkConnectionString, String kafkaConnectionString) {
        this.kafkaUnit = new KafkaUnit(zkConnectionString, kafkaConnectionString);
    }

    @Override
    protected void before() throws Throwable {
        kafkaUnit.startup();
    }

    @Override
    protected void after() {
        kafkaUnit.shutdown();
    }

    /**
     * Gets the ZooKeeper port
     * @return ZooKeeper port
     */
    public int getZkPort() {
        return kafkaUnit.getZkPort();
    }

    /**
     * Gets the Kafka port
     * @return Kafka port
     */
    public int getKafkaPort() {
        return kafkaUnit.getBrokerPort();
    }

    /**
     * Gets the underlying KafkaUnit
     * @return
     */
    public KafkaUnit getKafkaUnit() {
        return kafkaUnit;
    }
}
