package com.bigdatapassion.kafka.avro.writer;

import com.bigdatapassion.kafka.dto.ProductAvro;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.contentOf;

public class AvroRecordWriterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldWriteRecordToFile() throws IOException {
        File avroFile = folder.newFile("products.avro");
        String productName = "Product some name";
        ProductAvro product = ProductAvro.newBuilder()
                .setProductName(productName)
                .setPrice(String.valueOf(123))
                .setColor("Blue")
                .setMaterial("Wood")
                .setPromotionCode("")
                .build();

        // when
        AvroRecordWriter<ProductAvro> avroRecordWriter = new AvroRecordWriter<>(ProductAvro.class);
        avroRecordWriter.writeToFile(product, avroFile);

        // then
        assertThat(avroFile).exists().isFile();
        assertThat(contentOf(avroFile)).contains(productName);
    }

}