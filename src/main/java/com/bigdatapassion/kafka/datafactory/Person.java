package com.bigdatapassion.kafka.datafactory;

public class Person {

    private String name;
    private String phoneNumber;
    private String streetName;
    private String number;
    private String city;
    private String country;
    private String animal;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public String getAnimal() {
        return animal;
    }

    public String getStreetName() {
        return streetName;
    }

    public void setAnimal(String animal) {
        this.animal = animal;
    }

    public void setStreetName(String streetName) {
        this.streetName = streetName;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

}
