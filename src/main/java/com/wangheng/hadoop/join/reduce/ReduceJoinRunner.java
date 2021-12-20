package com.wangheng.hadoop.join.reduce;

import com.wangheng.hadoop.flowsum.FlowBean;
import com.wangheng.hadoop.flowsum.FlowSumMapper;
import com.wangheng.hadoop.flowsum.FlowSumReducer;
import com.wangheng.hadoop.flowsum.FlowSumRunner;
import com.wangheng.hadoop.join.TableBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author wangheng
 * @date 2021/12/16
 * @类职责
 * @设计文档
 */
public class ReduceJoinRunner {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ReduceJoinRunner.class);
        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(TableBean.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        Path inputPath = new Path("/Users/wangheng/workspace/bigdata/hadoop/src/main/resources/test_data/input");
        Path outputPath = new Path("/Users/wangheng/workspace/bigdata/hadoop/src/main/resources/test_output/reduce_join");
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath,true);
        }

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }
}
