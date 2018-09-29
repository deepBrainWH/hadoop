package com.wangheng.hadoop.secquence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;

public class SecquenceFileWriter {
    private static Configuration conf = new Configuration();
    private static String HDFS_URL = "hdfs://localhost:9000";

    private static String[] data = {"a,b,c,d,e,f,g", "e,f,g,h,i,j,k", "l,m,n,o,p,q,r,s", "t,u,v,w,x,y,z"};

    public static void main(String[] args) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(URI.create(HDFS_URL), conf, "wangheng");

        Path outputPath = new Path("/data/MySequenceFile.seq");
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, outputPath, IntWritable.class, Text.class);
        for(int i= 0;i<10;i++){
            key.set(10 - i);
            value.set(data[i%data.length]);
            writer.append(key, value);
        }
        IOUtils.closeStream(writer);
    }
}
