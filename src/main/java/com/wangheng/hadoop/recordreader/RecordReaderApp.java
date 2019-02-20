package com.wangheng.hadoop.recordreader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.net.URI;

public class RecordReaderApp {
    public static class MyRecordReader extends RecordReader<LongWritable, Text>{


        //start position
        private long start;

        //end position
        private long end;

        //current position
        private long pos;

        //file input stream
        private FSDataInputStream fin = null;

        //key, value
        private LongWritable key = null;
        private Text value = null;
        //defining a line reader
        private LineReader reader = null;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
                throws IOException, InterruptedException {
            //get the split file
            FileSplit split = (FileSplit) inputSplit;
            //get the start position
            start = split.getStart();
            end = start + split.getLength();
            Configuration conf = new Configuration();
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(conf);
            fin = fs.open(path);
            fin.seek(start);
            reader = new LineReader(fin);
            pos = 1;
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if(key == null){
                key = new LongWritable();
            }
            key.set(pos);
            if(value == null){
                value = new Text();
            }
            if(reader.readLine(value) == 0){
                return false;
            }
            pos ++;
            return true;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void close() throws IOException {
            fin.close();
        }
    }

    public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class MyReducer extends Reducer<LongWritable, Text, Text, LongWritable>{
        private Text outKey = new Text();
        private LongWritable outValue = new LongWritable();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            System.out.println("奇数行还是偶数行："+ key);
            //定义求和的变量
            long sum = 0;
            //遍历value求和
            for(Text val: values){
                sum += Long.parseLong(val.toString());
            }

            //判断奇数还是偶数
            if(key.get() == 0){
                outKey.set("奇数之和为：");
            }else {
                outKey.set("偶数之和为:");
            }
            //setting value
            outValue.set(sum);
            context.write(outKey, outValue);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String INPUT_PATH = "hdfs://localhost:9000/user/wangheng/data";
        String OUTPUT_PATH = "hdfs://localhost:9000/user/wangheng/output";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        final FileSystem fs = FileSystem.get(URI.create(INPUT_PATH), conf, "wangheng");
        if(fs.exists(new Path(OUTPUT_PATH)))
            fs.delete(new Path(OUTPUT_PATH), true);

        Job job = Job.getInstance(conf, "recordReader");

        job.setJarByClass(RecordReaderApp.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置输入目录和数据格式化的类
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        job.setInputFormatClass(MyInputFormat.class);

        //设置分区和reducer数量
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(2);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //指定输出路径和输出的格式化类
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
