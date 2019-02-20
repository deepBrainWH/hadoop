package com.wangheng.hadoop.sort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {

    private String phoneNumber;
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public FlowBean() {
    }//In java reflect, it's needing to
    // construct a void construct function

    public FlowBean(String phoneNumber, long upFlow, long downFlow, long sumFlow) {
        this.phoneNumber = phoneNumber;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //将对象数据序列化到对象的数据
        dataOutput.writeUTF(phoneNumber);
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //将数据反序列化到对象中
        phoneNumber = dataInput.readUTF();
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }

    @Override
    public String toString(){
        return this.phoneNumber+"\t"+this.upFlow+"\t"+this.downFlow+"\t"+this.sumFlow;
    }

    @Override
    public int compareTo(FlowBean o) {
        long thisValue = this.sumFlow;
        long thatValue = o.sumFlow;
        return (thisValue > thatValue ? -1 : ( thisValue==thatValue ? 0 : 1));
    }
}
