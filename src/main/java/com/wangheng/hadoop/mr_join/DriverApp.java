package com.wangheng.hadoop.mr_join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @Auther: wangheng
 * @Date: 2018-10-07 20:13
 * @Description:
 */
public class DriverApp {
    private static final String INPUT_PATH = "";
    private static final String OUTPUT_PATH = "";
    private static final String HDFS_URI = "hdfs://localhost:9000";

    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(URI.create(HDFS_URI), conf, "wangheng");
        if(fs.exists(new Path(OUTPUT_PATH)))
            fs.delete(new Path(OUTPUT_PATH), true);

    }
}
