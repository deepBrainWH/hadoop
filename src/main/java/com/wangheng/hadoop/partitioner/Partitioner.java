package com.wangheng.hadoop.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

public class Partitioner {

    private static class MyMapper
            extends Mapper<LongWritable, Text, Text, IntWritable>{

        private final IntWritable one = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer strs = new StringTokenizer(value.toString());
            while (strs.hasMoreTokens()){
                context.write(new Text(strs.nextToken()), one);
            }
        }
    }

    private static class MyReducer
            extends Reducer<Text, IntWritable, Text, IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum =0;
            for(IntWritable val:values){
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class MyPartitioner
            extends org.apache.hadoop.mapreduce.Partitioner<Text, IntWritable>{
        @Override
        public int getPartition(Text key, IntWritable intWritable, int i) {
            if(key.toString().startsWith("A") || key.toString().startsWith("a")){
                return 0;
            }else if(key.toString().startsWith("M") || key.toString().startsWith("m")){
                return 1;
            }else if(key.toString().startsWith("N") || key.toString().startsWith("m")){
                return 2;
            }
            return 3;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        Path inputPath = new Path("/data/word");
        Path outputPath = new Path("/data/partitioner_output");
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf, "wangheng");
        if(fs.exists(outputPath))
            fs.delete(outputPath, true);
        Job job = Job.getInstance(conf, "PartitionerApp");

        //set jar
        job.setJarByClass(Partitioner.class);

        //set mapper
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //set reducer
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //setting partitioner
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(4);

        //Input format
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);

        //output format
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
