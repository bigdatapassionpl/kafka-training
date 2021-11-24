package com.bigdatapassion.kafka.datafactory;

import com.bigdatapassion.kafka.dto.ProductAvro;
import com.bigdatapassion.kafka.dto.ProductMessageAvro;
import com.github.javafaker.Faker;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ProductMessageAvroFactory {

    private Faker faker = new Faker();
    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

    public ProductMessageAvro generateNextMessage(long number) {

        ProductMessageAvro message = new ProductMessageAvro();
        message.setId(number);
        message.setCreationDate(LocalDateTime.now().format(formatter));

        ProductAvro product = new ProductAvro();

        product.setProductName(faker.commerce().productName());
        product.setColor(faker.commerce().color());
        product.setMaterial(faker.commerce().material());
        product.setPrice(faker.commerce().price());
        product.setPromotionCode(faker.commerce().promotionCode());

        message.setProduct(product);
        return message;
    }

}
