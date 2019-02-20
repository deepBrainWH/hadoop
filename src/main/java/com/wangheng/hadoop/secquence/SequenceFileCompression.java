package com.wangheng.hadoop.secquence;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;

public class SequenceFileCompression {
    private static Configuration conf = new Configuration();
    private static Path path = new Path("MySequenceFileCompress.seq");
    private static FileSystem fs;

    static {
        try {
            String HDFS_PATH = "hdfs://localhost:9000";
            fs = FileSystem.get(URI.create(HDFS_PATH), conf, "wangheng");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static String[] data = {"a,b,c,d,e,f,g", "e,f,g,h,i,j,k", "l,m,n,o,p,q,r,s", "t,u,v,w,x,y,z"};

    public static void writeSequenceCompress() throws Exception{
        IntWritable key = new IntWritable();
        Text value = new Text();

        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class,
                SequenceFile.CompressionType.RECORD, new BZip2Codec());

        for(int i = 0;i<10;i++){
            key.set(10-i);
            value.set(data[i%data.length]);
            writer.append(key, value);
        }
        IOUtils.closeStream(writer);
    }

    public static void readSequenceCompress() throws Exception{
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Writable keyClass = (Writable)ReflectionUtils.newInstance(IntWritable.class, conf);
        Writable valueClass = (Writable)ReflectionUtils.newInstance(Text.class, conf);

        while (reader.next(keyClass, valueClass)){
            System.out.println("key:" + keyClass);
            System.out.println("value:" + valueClass);
            System.out.println("position:" + reader.getPosition());
        }
        IOUtils.closeStream(reader);
    }

    public static void main(String[] args) throws Exception {
//        writeSequenceCompress();
        readSequenceCompress();
    }


}
