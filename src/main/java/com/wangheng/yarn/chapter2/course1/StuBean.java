package com.wangheng.yarn.chapter2.course1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class StuBean implements WritableComparable<StuBean> {
    private String name;
    private String class_;
    private int score;

    public StuBean(String name, String class_, int score) {
        this.name = name;
        this.class_ = class_;
        this.score = score;
    }

    public StuBean() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClass_() {
        return class_;
    }

    public void setClass_(String class_) {
        this.class_ = class_;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @Override
    public int compareTo(StuBean o) {
        if(this.name.equals(o.getName())){
            return this.class_.compareTo(o.class_)>0?1:-1;
        }else {
            return this.name.compareTo(o.name)>0?1:-1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(class_);
        dataOutput.writeInt(score);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        name = dataInput.readUTF();
        class_ = dataInput.readUTF();
        score = dataInput.readInt();
    }

    @Override
    public String toString(){
        return this.name + "\t" + this.class_ + "\t" + this.score;
    }
}
