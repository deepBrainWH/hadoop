package com.wangheng.yarn.chapter2.course1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class MyReducer extends Reducer<StuBean, NullWritable, StuBean, NullWritable> {

    @Override
    protected void reduce(StuBean key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
