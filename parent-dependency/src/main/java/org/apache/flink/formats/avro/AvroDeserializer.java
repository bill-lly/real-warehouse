package org.apache.flink.formats.avro;

import org.apache.flink.avro.shaded.org.apache.avro.Schema;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.avro.shaded.org.apache.avro.generic.GenericData;
import org.apache.flink.avro.shaded.org.apache.avro.generic.GenericDatumReader;
import org.apache.flink.avro.shaded.org.apache.avro.generic.GenericRecord;
import org.apache.flink.avro.shaded.org.apache.avro.io.Decoder;
import org.apache.flink.avro.shaded.org.apache.avro.io.DecoderFactory;
import org.apache.flink.formats.avro.utils.MutableByteArrayInputStream;


import javax.annotation.Nullable;

import java.io.IOException;

public class AvroDeserializer implements DeserializationSchema<String> {


  private static final long serialVersionUID = -6766681879020862312L;

  private transient GenericDatumReader<GenericRecord> datumReader;

  private transient MutableByteArrayInputStream inputStream;

  private transient Decoder decoder;

  private final SchemaCoder.SchemaCoderProvider schemaCoderProvider;

  private transient SchemaCoder schemaCoder;

  public AvroDeserializer(SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
    this.schemaCoderProvider = schemaCoderProvider;
    this.schemaCoder = schemaCoderProvider.get();
  }

  GenericDatumReader<GenericRecord> getDatumReader() {
    return datumReader;
  }

  MutableByteArrayInputStream getInputStream() {
    return inputStream;
  }

  Decoder getDecoder() {
    return decoder;
  }

  @Override
  public String deserialize(@Nullable byte[] message) throws IOException {
    if (message == null) {
      return null;
    }
    checkAvroInitialized();
    getInputStream().setBuffer(message);
    Schema writerSchema = schemaCoder.readSchema(getInputStream());

    GenericDatumReader<GenericRecord> datumReader = getDatumReader();

    datumReader.setSchema(writerSchema);
    datumReader.setExpected(writerSchema);

    GenericRecord record = datumReader.read(null, getDecoder());
    return record.toString();
  }

  void checkAvroInitialized() {
    if (datumReader != null) {
      return;
    }

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    GenericData genericData = new GenericData(cl);
    this.datumReader = new GenericDatumReader<>(null, null, genericData);

    this.inputStream = new MutableByteArrayInputStream();
    this.decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    if (schemaCoder == null) {
      this.schemaCoder = schemaCoderProvider.get();
    }
  }

  @Override
  public boolean isEndOfStream(String nextElement) {
    return false;
  }

  @Override
  public TypeInformation<String> getProducedType() {
    return TypeInformation.of(String.class);
  }
}
