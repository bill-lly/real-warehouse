package org.apache.flink.formats.avro.registry.confluent;

import org.apache.flink.formats.avro.AvroDeserializer;

public class ConfluentAvroDeserializerFactory {

  public static AvroDeserializer forGeneric(String url) {
    return new AvroDeserializer(
        new CachedSchemaCoderProvider(null, url, 1000, null));
  }
}
