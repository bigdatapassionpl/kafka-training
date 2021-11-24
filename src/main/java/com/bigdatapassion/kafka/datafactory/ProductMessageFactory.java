package com.bigdatapassion.kafka.datafactory;

import com.bigdatapassion.kafka.dto.Product;
import com.bigdatapassion.kafka.dto.ProductMessage;
import com.github.javafaker.Faker;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ProductMessageFactory {

    private Faker faker = new Faker();
    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

    public ProductMessage generateNextMessage(long number) {

        ProductMessage message = new ProductMessage();
        message.setId(number);
        message.setCreationDate(LocalDateTime.now().format(formatter));

        Product product = new Product();

        product.setProductName(faker.commerce().productName());
        product.setColor(faker.commerce().color());
        product.setMaterial(faker.commerce().material());
        product.setPrice(faker.commerce().price());
        product.setPromotionCode(faker.commerce().promotionCode());

        message.setProduct(product);
        return message;
    }

}
