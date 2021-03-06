/*
 * Copyright (C) 2014 Nabarun Mondal
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

import java.util.Map;

/**
 * Created by noga on 25/12/16.
 * A wrapper to wrap the old vs new style of code
 * in Kafka, as KeyedMessage is deprecated
 */
public class KeyedMessage<K,V> {

    /**
     * The underlying record
     */
    public final ProducerRecord<K,V> record;

    /**
     * Creates a KeyedMessage
     * @param topic the topic
     * @param partition the number
     * @param timestamp time for the record
     * @param key of the record
     * @param value of the record
     */
    public KeyedMessage(String topic, Integer partition, Long timestamp, K key, V value){
        record = new ProducerRecord<>(topic,partition,timestamp,key,value);
    }

    /**
     * Creates a KeyedMessage with null timestamp
     * @param topic the topic
     * @param partition number
     * @param key attribute
     * @param value of the record
     */
    public KeyedMessage( String topic, Integer partition, K key, V value ){
        this( topic, partition, null, key, value);
    }

    /**
     * Creates a KeyedMessage with null partition and timestamp
     * @param topic the topic
     * @param key attribute
     * @param value of the record
     */
    public KeyedMessage(String topic, K key, V value) {
        this(topic, null, key, value);
    }

    /**
     * Creates a KeyedMessage with topic and value, rest all are null
     * @param topic the topic
     * @param value of the record
     */
    public KeyedMessage(String topic, V value) {
        this(topic, null, value);
    }

    /**
     * @return The topic this record is being sent to
     */
    public String topic() {
        return record.topic();
    }

    /**
     * @return The key (or null if no key is specified)
     */
    public K key() {
        return record.key() ;
    }

    /**
     * @return The value
     */
    public V value() {
        return record.value();
    }

    /**
     * @return The timestamp
     */
    public Long timestamp() {
        return record.timestamp();
    }

    /**
     * @return The partition to which the record will be sent (or null if no partition was specified)
     */
    public Integer partition() {
        return record.partition();
    }

    @Override
    public String toString() {
        return record.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if ( o instanceof ProducerRecord ){ return o.equals(record); }
        return ( o instanceof KeyedMessage ) &&
                record.equals( ((KeyedMessage)o).record ) ;
    }

    @Override
    public int hashCode() {
        return record.hashCode();
    }
}
