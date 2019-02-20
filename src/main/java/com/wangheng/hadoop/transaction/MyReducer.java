package com.wangheng.hadoop.transaction;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class MyReducer extends Reducer<Text, TrBean, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<TrBean> values, Context context)
            throws IOException, InterruptedException {
        Text outputValue = new Text();
        int contnum = 0;
        double contprice = 0;

        double sum_cc = 0;//(sigma(i=1, n)[CONTPRICE(ij) * CONTNUM(ij)]
        int sum_contnum = 0;
        double sum_contprice = 0;

        for(TrBean var: values){
            contnum = var.getContnum();

            contprice = var.getContprice();
            sum_cc += contnum * contprice;
            sum_contnum += contnum;
            sum_contprice += contprice;
        }
        System.out.println("key: "+key.toString() +"\t num: " + sum_contnum);
        double yi = sum_cc / sum_contnum;
        String outStr = sum_contprice + "\t" + sum_contnum + "\t" + yi;
        outputValue.set(outStr);
        context.write(key, outputValue);
    }
}
