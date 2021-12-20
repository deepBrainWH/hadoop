package com.wangheng.hadoop.mr_join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * @Auther: wangheng
 * @Date: 2018-10-07 20:13
 * @Description: mapper join option
 */
public class DriverApp {
    private static final String INPUT_PATH = "/user/wangheng/mapJoinData";
    private static final String OUTPUT_PATH = "/user/wangheng/mapJoinOutput";
    private static final String HDFS_URI = "hdfs://dell:9000";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://localhost:9000");
//        FileSystem fs = FileSystem.get(URI.create(HDFS_URI), conf, "wangheng");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(OUTPUT_PATH)))
            fs.delete(new Path(OUTPUT_PATH), true);

        Job job = Job.getInstance(conf, "mapper join");

        //set main class jar
        job.setJarByClass(DriverApp.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //set map output key and value class
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Employee.class);

        //set reducer output key and value class
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
