package com.wangheng.hadoop.sort;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.net.URI;

public class SortMR extends Configured implements Tool {

    public static class SortMapper extends
            Mapper<LongWritable, Text, FlowBean, NullWritable>{
        //拿到一行数据，切分出各字段， 封装成一个FLowBean,作为key输出
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String fields[] = StringUtils.split(line, " ");
            String phoneNB = fields[0];
            System.out.println("key = [" + key + "], value = [" + value + "], context = [" + context + "]");
            long upFlow = Long.parseLong(fields[1]);
            long downFlow = Long.parseLong(fields[2]);
            context.write(new FlowBean(phoneNB, upFlow, downFlow, upFlow+downFlow), NullWritable.get());
        }
    }

    public static class SortReducer extends
            Reducer<FlowBean, NullWritable, FlowBean, NullWritable>{
        @Override
        protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {

            context.write(key, NullWritable.get());
        }
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String HDFS_DIR = "hdfs://localhost:9000";
        conf.set("fs.defaultFS", HDFS_DIR);
        Job job = Job.getInstance(conf);

        job.setJarByClass(SortMR.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(NullWritable.class);

        Path inputPath = new Path(HDFS_DIR + "/data/flow");
        Path outputPath = new Path(HDFS_DIR + "/data/output");

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(new URI(HDFS_DIR), conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        System.exit(job.waitForCompletion(true)?0:1);
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(),new SortMR(), args);
        System.exit(res);
    }
}
