package com.gs.cloud.warehouse.robot.iot.format;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.Map;

public class GenericRecordDeserializer implements KafkaRecordDeserializationSchema<String> {

    private final String schemaUrl;
    protected KafkaAvroDeserializer avroDeserializer;

    public GenericRecordDeserializer(String schemaUrl) {
        this.schemaUrl = schemaUrl;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out) {
        GenericRecord genericRecord = (GenericRecord) avroDeserializer.deserialize("", record.value());
        if (null != genericRecord) {
            out.collect(genericRecord.toString());
        }
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        avroDeserializer = new KafkaAvroDeserializer();
        Map<String, Object> config = new HashMap<>();
        config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        config.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, false);
        config.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);
        avroDeserializer.configure(config, false);
    }


    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
