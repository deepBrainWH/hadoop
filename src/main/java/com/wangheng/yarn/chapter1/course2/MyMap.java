package com.wangheng.yarn.chapter1.course2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyMap extends Mapper<Object, Text, Text, NullWritable> {

    private Text word = new Text();
    private NullWritable outValue = NullWritable.get();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while (tokenizer.hasMoreTokens()){
            word.set(tokenizer.nextToken());
            context.write(word, outValue);
        }
    }
}
