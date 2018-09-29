package com.wangheng.hadoop.recordreader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

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
        public boolean nextKeyValue() throws IOException, InterruptedException {
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


        }
    }
}
