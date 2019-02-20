package com.wangheng.hadoop.recordreader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class MyInputFormat extends FileInputFormat<LongWritable, Text> {
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit,
                                                               TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        //return the recordReader, defined via yourself.
        return new RecordReaderApp.MyRecordReader();
    }

    /**
     * 为了使切分数据时不发生错乱，这里不进行切分
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
