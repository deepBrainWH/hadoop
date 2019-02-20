package com.wangheng.hadoop.flowsum;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    //
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //get one line data
        String line = value.toString();
        String[] fields = StringUtils.split(line, " ");
        String phoneNumber = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        context.write(new Text(phoneNumber), new FlowBean(phoneNumber,upFlow,downFlow, upFlow+downFlow));
    }
}
