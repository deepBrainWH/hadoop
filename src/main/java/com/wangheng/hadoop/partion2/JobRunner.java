package com.wangheng.hadoop.partion2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author wangheng
 * @date 2021/12/16
 * @类职责
 * @设计文档
 */
public class JobRunner {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        Job job = Job.getInstance(conf);

        job.setJarByClass(JobRunner.class);

        job.setInputFormatClass(TextInputFormat.class);
        //set mapper
        job.setMapperClass(PhoneMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //set reducer
        job.setReducerClass(PhoneReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //set partitioner
        job.setPartitionerClass(PhonePartioner.class);
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path("/Users/wangheng/workspace/bigdata/hadoop/src/main/resources/test_data/phone_data.txt"));

        Path outputPath = new Path("/Users/wangheng/workspace/bigdata/hadoop/src/main/resources/test_output/partitioner2");
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }
}
