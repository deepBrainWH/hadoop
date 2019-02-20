package com.wangheng.hadoop.transaction;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MyMapper extends Mapper<Object, Text, Text, TrBean> {

    private Text keyOut = new Text();

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] line = value.toString().split(",");
        String date = line[1];
        String contprice = line[5];
        String contnum = line[3];
        if(contprice.matches("\\d+[.]\\d+")){
            keyOut.set(date);
            context.write(keyOut, new TrBean(Integer.parseInt(contnum), Double.parseDouble(contprice)));
        }

    }
}
