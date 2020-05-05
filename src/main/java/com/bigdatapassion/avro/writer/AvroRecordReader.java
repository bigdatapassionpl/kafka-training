package com.bigdatapassion.avro.writer;

import com.google.common.base.Preconditions;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AvroRecordReader<T extends SpecificRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroRecordReader.class);

    private Class<T> clazz;

    public AvroRecordReader(final Class<T> clazz) {
        this.clazz = clazz;
    }

    public List<T> readFromFile(File file) {
        Preconditions.checkNotNull(file);
        List<T> records = new ArrayList<>();

        final DatumReader<T> datumReader = new SpecificDatumReader<>(clazz);
        final DataFileReader<T> dataFileReader;
        try {
            dataFileReader = new DataFileReader<>(file, datumReader);
            while (dataFileReader.hasNext()) {
                T record = dataFileReader.next();
                LOGGER.info("Reading our specific record {}", record);
                records.add(record);
            }
            return records;
        } catch (IOException e) {
            LOGGER.error("Could not read records from the file {}", file.getName());
            throw new RuntimeException(e);
        }
    }

}
