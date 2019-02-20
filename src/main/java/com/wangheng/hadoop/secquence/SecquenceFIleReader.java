package com.wangheng.hadoop.secquence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.net.URI;

public class SecquenceFIleReader {
    private static Configuration conf = new Configuration();
    private static String HDFS_URL = "hdfs://localhost:9000";

    public static void main(String[] args) throws Exception{
        FileSystem fs = FileSystem.get(URI.create(HDFS_URL), conf, "wangheng");
        Path inputPath = new Path(HDFS_URL + "/data/MySequenceFile.seq");

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, inputPath, conf);

        Writable keyClass = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(),conf);
        Writable valueClass = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);

        while (reader.next(keyClass, valueClass)){
            System.out.println("key: " + keyClass);
            System.out.println("value: " + valueClass);
            System.out.println("position: " + reader.getPosition());
        }
        IOUtils.closeStream(reader);
    }
}
