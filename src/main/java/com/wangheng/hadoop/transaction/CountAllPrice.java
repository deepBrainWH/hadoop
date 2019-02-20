package com.wangheng.hadoop.transaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class CountAllPrice {
    private static String HDFS = "hdfs://localhost:9000";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS);
        Job job = Job.getInstance(conf, "汇总");
        job.setJarByClass(CountAllPrice.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TrBean.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(5);

        Path inputPath = new Path(HDFS + "/transaction/");
        Path outputPath = new Path(HDFS + "/transaction/output/");

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(URI.create(HDFS), conf, "wangheng");
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
