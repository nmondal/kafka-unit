package info.batey.kafka.unit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Rule;
import org.junit.Test;
import java.util.Arrays;
import java.util.Properties;
import static org.junit.Assert.*;

/**
 */
public class GenericMessageTest {

    @Rule
    public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule();

    private boolean arrayEquals(byte[] a, byte[] b){
        if ( a.length != b.length ) return false;
        for ( int i = 0 ; i < a.length; i++ ){
            if ( a[i] != b[i]  ) return false ;
        }
        return true;
    }

    @Test
    public void sendReceive(){
        final String topic = "km-test" ;
        final String value = "Hello, KafkaUnit" ;
        final byte[] arr = new byte[]{ 0,1,2,3,4,5,6,7,8,9 } ;

        // The Production
        Properties producerProp = kafkaUnitRule.getKafkaUnit().producerDefault();
        producerProp.setProperty( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getCanonicalName());

        ProducerRecord<String,byte[]> m = new ProducerRecord<>(
                topic, value, arr);

        KafkaProducer p = kafkaUnitRule.getKafkaUnit().producer(producerProp);
        p.send( m );
        p.send( m );
        p.send( m );

        // The consumption
        Properties consumerProp = kafkaUnitRule.getKafkaUnit().consumerDefault();
        consumerProp.setProperty( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getCanonicalName());

        KafkaConsumer c = kafkaUnitRule.getKafkaUnit().consumer(consumerProp);
        c.subscribe(Arrays.asList(topic));

        ConsumerRecords cr = c.poll(10000);
        Iterable<ConsumerRecord> i = cr.records( topic );
        int count = 0 ;
        for ( ConsumerRecord<String,byte[]> r : i ){
            assertEquals(r.key(), value) ;
            assertTrue(arrayEquals( r.value(), arr ) ) ;
            count++;
        }
        assertEquals(3,count);
    }
}
