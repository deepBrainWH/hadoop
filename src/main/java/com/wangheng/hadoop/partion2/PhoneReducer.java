package com.wangheng.hadoop.partion2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author wangheng
 * @date 2021/12/16
 * @类职责
 * @设计文档
 */
public class PhoneReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0L;
        long sumDownFlow = 0L;
        for (FlowBean flowBean : values) {
            sumUpFlow += flowBean.getUpFlow();
            sumDownFlow += flowBean.getDownFlow();
        }
        context.write(key, new FlowBean(key.toString(), sumUpFlow, sumDownFlow));
    }
}
