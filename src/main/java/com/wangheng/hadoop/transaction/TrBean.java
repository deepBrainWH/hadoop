package com.wangheng.hadoop.transaction;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TrBean implements Writable{

    private int contnum;
    private double contprice;

    public TrBean() {
    }

    public TrBean(int contnum, double contprice) {
        this.contnum = contnum;
        this.contprice = contprice;
    }

    public int getContnum() {
        return contnum;
    }

    public void setContnum(int contnum) {
        this.contnum = contnum;
    }

    public double getContprice() {
        return contprice;
    }

    public void setContprice(double contprice) {
        this.contprice = contprice;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(contnum);
        dataOutput.writeDouble(contprice);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        contnum = dataInput.readInt();
        contprice = dataInput.readDouble();
    }

    @Override
    public String toString(){
        return this.contnum + "\t" + this.contprice;
    }
}
