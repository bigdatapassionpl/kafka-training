package com.bigdatapassion.kafka.dto;


public class PersonMessage {

    private Long id;
    private String creationDate;
    private Person person;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(String creationDate) {
        this.creationDate = creationDate;
    }

    public Person getPerson() {
        return person;
    }

    public void setPerson(Person person) {
        this.person = person;
    }

    @Override
    public String toString() {
        return "PersonMessage{" +
                "id=" + id +
                ", creationDate='" + creationDate + '\'' +
                ", person=" + person +
                '}';
    }

}