package com.wangheng.yarn.chapter1.course1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static IntWritable result = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable one: values){
            sum += one.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
