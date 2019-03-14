package com.wangheng.yarn.chapter1.course1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class MyRunner {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://dell:9000");
        try {
            FileSystem fs = FileSystem.get(conf);
            Job job = Job.getInstance(conf, "chapter-1:course-1");
            job.setJarByClass(MyRunner.class);

            job.setMapperClass(MyMap.class);
            job.setReducerClass(MyReduce.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path("/hadoop_test_data/chapter_1/"));
            Path output = new Path("/hadoop_test_data/chapter_1/course_1/");
            if(fs.exists(output)){
                fs.delete(output, true);
            }
            FileOutputFormat.setOutputPath(job, output);

            System.exit(job.waitForCompletion(true)? 0: 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
