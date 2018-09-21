package com.wangheng.hadoop.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //具体业务逻辑就写在这个方法中，而且我们的要处理的数据已经被框架传递进来，
        // 在方法的参数中key-vlue
        //key是这一行数据的起始偏移量， value是这一行的文本内容
        //将这一行的内容转换为String类型
        String line = value.toString();
        //将文本按特定的切割福切割
        String[] words = StringUtils.split(line, " ");
        for(String word: words){
            context.write(new Text(word), new LongWritable(1));
        }
    }
}
