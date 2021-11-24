package com.bigdatapassion.kafka.avro;

import com.bigdatapassion.kafka.dto.Person;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import org.apache.avro.Schema;

public class SchemaGenerator {

    public static void main(String[] args) throws Exception {
        Class<Person> type = Person.class;

        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator gen = new AvroSchemaGenerator();
        mapper.acceptJsonFormatVisitor(type, gen);
        AvroSchema schemaWrapper = gen.getGeneratedSchema();
        Schema avroSchema = schemaWrapper.getAvroSchema();
        String avroSchemaAsJson = avroSchema.toString(true);

        System.out.println(avroSchemaAsJson);
    }

}
