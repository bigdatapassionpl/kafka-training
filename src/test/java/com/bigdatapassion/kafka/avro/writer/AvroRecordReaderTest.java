package com.bigdatapassion.kafka.avro.writer;

import com.bigdatapassion.Product;
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
        Product product = Product.newBuilder()
                .setName(productName)
                .setPrice(123)
                .build();

        AvroRecordWriter<Product> avroRecordWriter = new AvroRecordWriter<>(Product.class);
        avroRecordWriter.writeToFile(product, avroFile);

        // when
        AvroRecordReader<Product> avroRecordReader = new AvroRecordReader<>(Product.class);
        List<Product> products = avroRecordReader.readFromFile(avroFile);

        // then
        assertThat(products).isNotEmpty();
        assertThat(products.get(0).getName()).isEqualTo(productName);
        assertThat(products.get(0)).isEqualByComparingTo(product);
    }

}