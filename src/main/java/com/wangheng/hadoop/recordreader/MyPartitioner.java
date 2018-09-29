package com.wangheng.hadoop.recordreader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Auther: wangheng
 * @Date: 2018-09-29 20:45
 * @Description:
 */
public class MyPartitioner extends Partitioner<LongWritable, Text> {
    @Override
    public int getPartition(LongWritable key, Text value, int i) {
        if(key.get() % 2 == 0){
            //偶数放到第二个分区计算
            key.set(1);
            return 1;
        }else {
            //奇数放到第一个分区计算
            key.set(0);
            return 0;
        }
    }
}
