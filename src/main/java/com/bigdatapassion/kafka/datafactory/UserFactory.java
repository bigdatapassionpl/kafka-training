package com.bigdatapassion.kafka.datafactory;

import com.bigdatapassion.kafka.json.User;
import com.github.javafaker.Faker;

import java.time.format.DateTimeFormatter;

public class UserFactory {

    private Faker faker = new Faker();
    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

    public User generateNextMessage(long number) {

        User user = new User();
        user.setEmail(faker.internet().emailAddress());

        return user;
    }

}
