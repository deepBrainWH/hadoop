package com.wangheng.hadoop.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCRedece extends Reducer<Text, LongWritable, Text, LongWritable> {

    /**
     * 框架在map处理完成后，将所有key-value对缓存起来，进行分组，然后传递一个组<key, values{}>
     * 调用一次reduce方法
     * <hello, {1,1,1,1,1,.......}>
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long count = 0;
        //遍历value的list进行累加求和
        for(LongWritable value: values){
            count += value.get();
        }
        //print the number of word
        context.write(key, new LongWritable(count));
    }
}
