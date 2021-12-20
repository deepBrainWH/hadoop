package com.wangheng.hadoop.partion2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wangheng
 * @date 2021/12/16
 * @类职责
 * @设计文档
 */
public class PhoneMapper extends Mapper<LongWritable, Text, Text,FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\\s+");
        String phoneNumber = split[1];
        long upFlow = Long.parseLong(split[3]);
        long downFlow = Long.parseLong(split[4]);
        context.write(new Text(phoneNumber), new FlowBean(phoneNumber,upFlow,downFlow));
    }
}
