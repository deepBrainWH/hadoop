package com.wangheng.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 用来描述一个作业，比如用那个类作为逻辑处理中的map,哪个作为reduce
 * 还可以制定作业要处理的数据所在路径
 * 以及结果存储路径
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://localhost:9000/");
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);

        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCRedece.class);

        //设置reduce的输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //设置mapper的数据输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //指定原始数据存放路径
        FileInputFormat.setInputPaths(job, "/home/wangheng/Desktop/test_data/test_data2.txt");
        FileOutputFormat.setOutputPath(job, new Path("/home/wangheng/Desktop/test_data/output"));

        job.waitForCompletion(true);
    }
}
