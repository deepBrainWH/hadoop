package com.wangheng.yarn.chapter2.course1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class Runner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://dell:9000");
        FileSystem fs = FileSystem.get(URI.create("hdfs://dell:9000"), conf, "wangheng");
        Job job = Job.getInstance(conf, "secondary-sort");

        job.setJarByClass(Runner.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(StuBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(StuBean.class);
        job.setOutputValueClass(NullWritable.class);

        Path inputPath = new Path("/hadoop_test_data/chapter_2/");
        Path outputPath = new Path("/hadoop_app_out/chapter_2");
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
