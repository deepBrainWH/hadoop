package com.wangheng.yarn.chapter2.course1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class MyMapper extends Mapper<Object, Text, StuBean, NullWritable> {
    @Override
    protected void map(Object key, Text value, Context context) {
        try{
            String[] stu = value.toString().split(" ");
            StuBean stuBean = new StuBean(stu[0], stu[1], Integer.parseInt(stu[2]));
            context.write(stuBean, NullWritable.get());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
