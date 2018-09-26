package com.wangheng.hadoop.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean>{

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {
        long u_flow_counter = 0;
        long d_flow_Counter = 0;

        for(FlowBean bean : values){
            u_flow_counter += bean.getUpFlow();
            d_flow_Counter += bean.getDownFlow();
        }

        context.write(key, new FlowBean(key.toString(), u_flow_counter,
                d_flow_Counter, u_flow_counter+d_flow_Counter));
    }
}
