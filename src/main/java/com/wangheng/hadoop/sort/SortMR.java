package com.wangheng.hadoop.sort;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
        conf.set("fs.defaultFS", "hdfs://localhost:9000/");
        Job job = Job.getInstance(conf);

        job.setJarByClass(SortMR.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(NullWritable.class);

        //args[0]
        //args[1]
        FileInputFormat.setInputPaths(job, new Path("/data/flow"));
        FileOutputFormat.setOutputPath(job, new Path("/data/output"));

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(),new SortMR(), args);
        System.exit(res);
    }
}
