package com.wangheng.hadoop.mr_join;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther: wangheng
 * @Date: 2018-10-07 19:02
 * @Description:
 */
public class Employee implements WritableComparable {
    private String empNo = "";
    private String empName = "";
    private String deptNo = "";
    private String deptName = "";
    private int flag = 0;


    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.empNo);
        dataOutput.writeUTF(this.empName);
        dataOutput.writeUTF(this.deptNo);
        dataOutput.writeUTF(this.deptName);
        dataOutput.writeInt(this.flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.empNo = dataInput.readUTF();
        this.empName = dataInput.readUTF();
        this.deptNo = dataInput.readUTF();
        this.deptName = dataInput.readUTF();
        this.flag = dataInput.readInt();
    }

    @Override
    public String toString() {
        return this.empNo + "\t" + this.empName + "\t" + this.deptNo + "\t" + this.deptName;
    }

    public Employee(String empNo, String empName, String deptNo, String deptName, int flag) {
        this.empNo = empNo;
        this.empName = empName;
        this.deptNo = deptNo;
        this.deptName = deptName;
        this.flag = flag;
    }

    public Employee() {
    }

    public String getEmpNo() {
        return empNo;
    }

    public void setEmpNo(String empNo) {
        this.empNo = empNo;
    }

    public String getEmpName() {
        return empName;
    }

    public void setEmpName(String empName) {
        this.empName = empName;
    }

    public String getDeptNo() {
        return deptNo;
    }

    public void setDeptNo(String deptNo) {
        this.deptNo = deptNo;
    }

    public String getDeptName() {
        return deptName;
    }

    public void setDeptName(String deptName) {
        this.deptName = deptName;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }
}
