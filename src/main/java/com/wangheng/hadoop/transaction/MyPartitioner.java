package com.wangheng.hadoop.transaction;

import org.apache.hadoop.io.DoubleWritable;;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, TrBean> {
    @Override
    public int getPartition(Text o, TrBean o2, int i) {
        String str = o.toString().substring(0, 4);
        if(str.equals("2009")){
            return 0;
        }else if(str.equals("2010")){
            return 1;
        }else if(str.equals("2011")){
            return 2;
        }else if(str.equals("2012")) {
            return 3;
        }
        return 4;
    }
}
