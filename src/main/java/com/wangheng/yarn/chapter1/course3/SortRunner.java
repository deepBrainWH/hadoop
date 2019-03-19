package com.wangheng.yarn.chapter1.course3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class SortRunner {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://dell:9000");
        FileSystem fs = FileSystem.get(URI.create("hdfs://dell:9000"), conf, "wangheng");
        Path inputPath = new Path("/hadoop_test_data/chapter_1/");
        Path outputPath = new Path("/hadoop_app_out/chapter_1/course_3");

        Job job = Job.getInstance(conf, "course_3:sort_key");
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setJarByClass(SortRunner.class);
        FileInputFormat.addInputPath(job, inputPath);
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(false)?0:1);
    }
}
