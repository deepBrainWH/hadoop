package com.wangheng.hadoop.mr_join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Auther: wangheng
 * @Date: 2018-10-07 19:18
 * @Description:
 */
public class MyReducer extends Reducer<LongWritable, Employee, NullWritable, Text> {

    @Override
    protected void reduce(LongWritable key, Iterable<Employee> values, Context context)
            throws IOException, InterruptedException {
        Employee dept = null;
        List<Employee> list = new ArrayList<Employee>();
        for(Employee e : values){
            if(e.getFlag() == 0){//emp.txt
                list.add(e);
            }else { // devp.txt
                dept = e;
            }
        }
        if(dept != null){
            for(Employee e : list){
                context.write(NullWritable.get(), new Text(e.toString()));
            }
        }
    }
}
