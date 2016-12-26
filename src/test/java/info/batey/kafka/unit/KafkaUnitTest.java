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


import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;


public class KafkaUnitTest {

    @Test
    public void successfullyConstructKafkaUnitFromConnectionStrings() {
        new KafkaUnit("localhost:2181", "localhost:9092");
    }

    @Test(expected = RuntimeException.class)
    public void failToConstructKafkaUnitFromZooKeeperConnectionStringWithNonNumericPort() {
        new KafkaUnit("localhost:abcd", "localhost:9092");
    }

    @Test(expected = RuntimeException.class)
    public void failToConstructKafkaUnitFromKafkaConnectionStringWithNonNumericPort() {
        new KafkaUnit("localhost:2181", "localhost:abcd");
    }

    @Test(expected = RuntimeException.class)
    public void failToConstructKafkaUnitFromZooKeeperConnectionStringWithMultipleHosts() {
        new KafkaUnit("localhost:2181,localhost:2182", "localhost:9092");
    }

    @Test(expected = RuntimeException.class)
    public void failToConstructKafkaUnitFromKafkaConnectionStringWithMultipleHosts() {
        new KafkaUnit("localhost:2181", "localhost:9092,localhost:9093");
    }

    @Test(expected = RuntimeException.class)
    public void failToConstructKafkaUnitFromInvalidZooKeeperConnectionString() {
        new KafkaUnit("localhost-2181", "localhost:9092");
    }

    @Test(expected = RuntimeException.class)
    public void failToConstructKafkaUnitFromInvalidKafkaConnectionString() {
        new KafkaUnit("localhost:2181", "localhost-9092");
    }

    @Test
    public void successfullyConstructKafkaUnitWithoutAnyParams() throws Exception {
        new KafkaUnit();
    }

    @Test
    public void testKeyedMessageWrapper(){
        KeyedMessage km = new KeyedMessage( "topic",
                0, System.nanoTime(), "k1", "v1" );
        ProducerRecord r = new ProducerRecord( "topic",
                0, km.timestamp() , "k1", "v1" );

        assertEquals( km, r);
        assertEquals(km,km.record );
        assertEquals( km, km );

        KeyedMessage k2 = new KeyedMessage( "topic",
                0, System.nanoTime(), "k1", "v1" );
        // should be false
        assertNotEquals(km,k2 );
        assertNotEquals(k2 , null );
    }
}
