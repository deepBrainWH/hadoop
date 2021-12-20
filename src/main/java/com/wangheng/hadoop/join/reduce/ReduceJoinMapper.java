package com.wangheng.hadoop.join.reduce;

import com.wangheng.hadoop.join.TableBean;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author wangheng
 * @date 2021/12/16
 * @类职责
 * @设计文档
 */
public class ReduceJoinMapper extends Mapper<LongWritable,Text, IntWritable, TableBean> {
    private String filename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        filename = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\\s+");
        IntWritable outKey;
        TableBean outV = new TableBean();
        if(filename.contains("order")){
            outKey = new IntWritable(Integer.parseInt(split[1]));
            outV.setOid(Integer.parseInt(split[0]));
            outV.setPid(Integer.parseInt(split[1]));
            outV.setCount(Integer.parseInt(split[2]));
            outV.setFlag("order");
        }else{
            outKey = new IntWritable(Integer.parseInt(split[0]));
            outV.setPid(Integer.parseInt(split[0]));
            outV.setName(split[1]);
            outV.setFlag("pd");
        }
        context.write(outKey, outV);
    }
}
