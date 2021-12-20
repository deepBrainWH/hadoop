package com.wangheng.hadoop.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author wangheng
 * @date 2021/12/16
 */
public class TableBean implements Writable {
    private int oid;
    private int pid;
    private int count;
    private String name;
    private String flag;//标记数据来源于哪张表

    public TableBean() {
        oid = 0;
        pid = 0;
        count = 0;
        name = "";
        flag = "";
    }

    public TableBean(int oid, int pid, int count, String name,String flag) {
        this.oid = oid;
        this.pid = pid;
        this.count = count;
        this.name = name;
        this.flag = flag;
    }

    public int getOid() {
        return oid;
    }

    public void setOid(int oid) {
        this.oid = oid;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(oid);
        dataOutput.writeInt(pid);
        dataOutput.writeInt(count);
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        oid = dataInput.readInt();
        pid = dataInput.readInt();
        count = dataInput.readInt();
        name = dataInput.readUTF();
        flag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return oid+"\t"+name+"\t"+count;
    }
}
