package com.wangheng.hadoop.serialize;

import java.io.IOException;

public class TestSerialization {

    public static void main(String[] args) throws IOException {

        //serialization
        Person p = new Person("wangheng", 21, "男");
        byte[] values =  SerializationUtil.serialize(p);

        //de-serialization
        Person person = new Person();
        SerializationUtil.deserialize(person, values);
        System.out.println(person.toString());

    }
}
