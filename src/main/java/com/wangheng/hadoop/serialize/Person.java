package com.wangheng.hadoop.serialize;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Person implements WritableComparable<Person> {

    private String name;
    private Integer age;
    private String gender;

    public Person(String name, Integer age, String gender) {
        this.name = name;
        this.age = age;
        this.gender = gender;
    }

    public Person() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    @Override
    public String toString() {
        return this.name + "\t" + this.gender + "\t" + this.age;
    }

    @Override
    public int compareTo(Person o) {
        int result = 0;
        int comp1 = this.name.compareTo(o.name);
        if(comp1 != 0){
            return comp1;
        }
        int comp2 = this.age.compareTo(o.age);
        if(comp2 != 0)
            return comp2;
        int comp3 = this.gender.compareTo(o.gender);
        if(comp3 != 0){
            return comp3;
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.name);
        dataOutput.writeInt(this.age);
        dataOutput.writeUTF(this.gender);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        name = dataInput.readUTF();
        age = dataInput.readInt();
        gender = dataInput.readUTF();
    }
}
