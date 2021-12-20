package com.wangheng.hadoop.join.reduce;

import com.wangheng.hadoop.join.TableBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @author wangheng
 * @date 2021/12/16
 * @类职责
 * @设计文档
 */
public class ReduceJoinReducer extends Reducer<IntWritable, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> orders = new ArrayList<>();
        TableBean pd = new TableBean();
        for (TableBean table : values) {
            if ("order".equals(table.getFlag())) {
                // 这里需要注意：hadoop为了优化使用了对象复用的技术，values存储的是地址，不是具体的对象
                TableBean dist = new TableBean();
                try {
                    BeanUtils.copyProperties(dist, table);
                    orders.add(dist);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    BeanUtils.copyProperties(pd, table);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        for (TableBean bean : orders) {
            bean.setName(pd.getName());
            context.write(bean, NullWritable.get());
        }
    }
}
