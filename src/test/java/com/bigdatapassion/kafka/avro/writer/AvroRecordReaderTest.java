package com.bigdatapassion.kafka.avro.writer;

import com.bigdatapassion.kafka.dto.ProductAvro;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AvroRecordReaderTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldReadAvroFile() throws IOException {
        File avroFile = folder.newFile("products.avro");
        String productName = "Product some name";
        ProductAvro product = ProductAvro.newBuilder()
                .setProductName(productName)
                .setPrice(String.valueOf(123))
                .build();

        AvroRecordWriter<ProductAvro> avroRecordWriter = new AvroRecordWriter<>(ProductAvro.class);
        avroRecordWriter.writeToFile(product, avroFile);

        // when
        AvroRecordReader<ProductAvro> avroRecordReader = new AvroRecordReader<>(ProductAvro.class);
        List<ProductAvro> products = avroRecordReader.readFromFile(avroFile);

        // then
        assertThat(products).isNotEmpty();
        assertThat(products.get(0).getProductName()).isEqualTo(productName);
        assertThat(products.get(0)).isEqualByComparingTo(product);
    }

}