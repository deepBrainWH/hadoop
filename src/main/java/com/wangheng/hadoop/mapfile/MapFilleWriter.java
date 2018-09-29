package com.wangheng.hadoop.mapfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.Writer;
import java.net.URI;

public class MapFilleWriter {
    static String HDFS_PATH = "hdfs://localhost:9000";
    static Configuration conf = new Configuration();
    static FileSystem fs;


    /**
     * MapFile writer to file
     * @throws IOException
     */
    public static void writer() throws IOException {
        try {
            fs = FileSystem.get(URI.create(HDFS_PATH), conf, "wangheng");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Path output = new Path(HDFS_PATH + "/user/wangheng/MapFile.map");
        Text key = new Text();
        key.set("myKey");
        Text value = new Text();
        value.set("myValue");
        MapFile.Writer writer = new MapFile.Writer(conf, fs, output.toString(),Text.class, Text.class);
        writer.append(key, value);
        IOUtils.closeStream(writer);
    }

    /**
     * MapFile read a file
     * @throws IOException
     */
    public static void read() throws Exception{
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        fs = FileSystem.get(URI.create(HDFS_PATH), conf, "wangheng");
        Path inpath = new Path("MapFile.map");
        MapFile.Reader reader = new MapFile.Reader(fs, inpath.toString(), conf);
        Writable keyClass = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable valueClass = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

        while (reader.next((WritableComparable)keyClass, valueClass)){
            System.out.println(keyClass);
            System.out.println(valueClass);
        }
        IOUtils.closeStream(reader);

    }

    public static void main(String[] args) throws Exception {
//        writer();
        read();
    }



}
