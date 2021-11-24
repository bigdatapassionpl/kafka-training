package com.bigdatapassion.kafka.avro.writer;

import com.google.common.base.Preconditions;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class AvroRecordWriter<T extends SpecificRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroRecordWriter.class);

    private Class<T> clazz;

    public AvroRecordWriter(final Class<T> clazz) {
        this.clazz = clazz;
    }

    public void writeToFile(T record, File file) {
        Preconditions.checkNotNull(record);
        Preconditions.checkNotNull(file);

        final DatumWriter<T> datumWriter = new SpecificDatumWriter<>(clazz);

        try (DataFileWriter<T> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(record.getSchema(), file);
            dataFileWriter.append(record);
            LOGGER.info("Successfully wrote file {}", file.getName());
        } catch (IOException e) {
            LOGGER.error("Could not write record to file {}", file.getName());
            throw new RuntimeException(e);
        }
    }

}
