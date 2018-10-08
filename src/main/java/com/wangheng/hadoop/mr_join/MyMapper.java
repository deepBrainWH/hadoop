package com.wangheng.hadoop.mr_join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther: wangheng
 * @Date: 2018-10-07 19:10
 * @Description:
 */
public class MyMapper extends Mapper<LongWritable, Text, LongWritable, Employee> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String val = value.toString();
        String []arr = val.split("\\t");
        System.out.println("arr.length = " + arr.length + ", arr[0] = " + arr[0]);

        if(arr.length <= 3){//dept.txt
            Employee e = new Employee();
            e.setDeptNo(arr[0]);
            e.setDeptName(arr[1]);
            e.setFlag(1);
            context.write(new LongWritable(Long.valueOf(e.getDeptNo())), e);
        }else {//emp.txt
            Employee e = new Employee();
            e.setEmpNo(arr[0]);
            e.setEmpName(arr[1]);
            e.setDeptNo(arr[6]);
            e.setFlag(0);
            context.write(new LongWritable(Long.valueOf(e.getDeptNo())), e);
        }
    }
}
