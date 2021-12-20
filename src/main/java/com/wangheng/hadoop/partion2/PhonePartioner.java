package com.wangheng.hadoop.partion2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author wangheng
 * @date 2021/12/16
 * @类职责
 * @设计文档
 */
public class PhonePartioner extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String prefixPhone = text.toString().substring(0, 3);
        switch (prefixPhone) {
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
        }
        return 4;
    }
}
