package com.bigdatapassion.kafka.datafactory;

import com.github.javafaker.Faker;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class PersonMessageFactory {

    private Faker faker = new Faker();
    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

    public PersonMessage generateNextMessage(int number) {

        PersonMessage message = new PersonMessage();
        message.setCreationDate(LocalDateTime.now().format(formatter));

        Person person = new Person();

        person.setName(faker.name().fullName());
        person.setPhoneNumber(faker.phoneNumber().phoneNumber());

        person.setCountry(faker.address().country());
        person.setCity(faker.address().city());
        person.setStreetName(faker.address().streetName());
        person.setNumber(faker.address().buildingNumber());

        person.setAnimal(faker.animal().name());

        message.setPerson(person);
        return message;
    }

}
