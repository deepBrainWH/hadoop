package com.wangheng.hadoop.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

public class WordCountCombinerAPP {
    public static class TokerizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()){
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }

        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);

        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountCombinerAPP.class);
        job.setMapperClass(TokerizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setCombinerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("/Users/wangheng/workspace/bigdata/hadoop/src/main/resources/test_data/word_count.txt"));
        Path outPath = new Path("/Users/wangheng/workspace/bigdata/hadoop/src/main/resources/test_output/wordcount");
        FileOutputFormat.setOutputPath(job, outPath);

        FileSystem fs = FileSystem.get( conf);
        if(fs.exists(outPath)){
            fs.delete(outPath, true);
        }
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
