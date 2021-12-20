package com.wangheng.yarn.chapter1.course2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class DeduplicationRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://dell:9000");
        //FileSystem fs = FileSystem.get(URI.create("hdfs://dell:9000"), conf, "wangheng");
        Job job = Job.getInstance(conf, "deduplication");

        job.setJarByClass(DeduplicationRunner.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);

        FileInputFormat.addInputPath(job, new Path("/hadoop_test_data/chapter_1/"));
        Path outputPath = new Path("/hadoop_app_out/chapter1/course2");
//        if(fs.exists(outputPath)){
//            fs.delete(outputPath, true);
//        }
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
