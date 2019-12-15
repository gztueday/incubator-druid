package org.apache.druid.data.input.avro;

import com.batmobi.dataxsync.connect.avro.AvroConverter;
import com.batmobi.dataxsync.serializers.AbstractKafkaAvroSerDe;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZYSchemaRegistryBasedAvroBytesDecoder implements AvroBytesDecoder {
   private final AvroConverter converter;
   private static final Logger log = new Logger(ZYSchemaRegistryBasedAvroBytesDecoder.class);

   @JsonCreator
   public ZYSchemaRegistryBasedAvroBytesDecoder(
           @JsonProperty("url") String url,
           @JsonProperty("capacity") Integer capacity
   ) {
      log.info("注册的schemal的地址为:[{}]", url);
      this.converter = new AvroConverter();
      Map<String, String> configs = new HashMap<>(1);
      configs.put("schema.registry.url", url);
      converter.configure(configs, false);
   }

   /**
    * 默认是不用的
    *
    * @param registry
    */
   //For UT only
   @VisibleForTesting
   ZYSchemaRegistryBasedAvroBytesDecoder(SchemaRegistryClient registry) {
      this.converter = new AvroConverter();
      Map<String, String> configs = new HashMap<>();
      configs.put("schema.registry.url", "http://datax-sync.hk.batmobi.cn");
      converter.configure(configs, false);
   }

   @Override
   public GenericRecord parse(ByteBuffer bytes) {
      try {
         bytes.get(); // ignore first \0 byte
         int id = bytes.getInt(); // extract schema registry id
         int length = bytes.limit() - 1 - AbstractKafkaAvroSerDe.idSize;
         int offset = bytes.position() + bytes.arrayOffset();
         final Schema schema = this.converter.schemaRegistry.getById(id);
         final DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
         return reader.read(null, DecoderFactory.get().binaryDecoder(bytes.array(), offset, length, null));
      } catch (Exception e) {
         throw new ParseException(e, "Fail to decode avro message!");
      }
   }

   @Override
   public List<GenericRecord> parseBatch(final ByteBuffer bytes) {
      try {
         bytes.get(); // ignore first \0 byte
         int id = bytes.getInt(); // extract schema registry id
         int length = bytes.limit() - 1 - AbstractKafkaAvroSerDe.idSize;
         int offset = bytes.position() + bytes.arrayOffset();
         final Schema schema = this.converter.schemaRegistry.getById(id);
         final DatumReader<List<GenericRecord>> reader = new GenericDatumReader<>(schema);
         return reader.read(null, DecoderFactory.get().binaryDecoder(bytes.array(), offset, length, null));
      } catch (Exception e) {
         throw new ParseException(e, "Fail to decode avro message!");
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      ZYSchemaRegistryBasedAvroBytesDecoder that = (ZYSchemaRegistryBasedAvroBytesDecoder) o;

      return converter != null ? converter.equals(that.converter) : that.converter == null;
   }

   @Override
   public int hashCode() {
      return converter != null ? converter.hashCode() : 0;
   }
}
