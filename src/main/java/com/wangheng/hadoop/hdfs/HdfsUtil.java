package com.wangheng.hadoop.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.FileOutputStream;
import java.io.IOException;

public class HdfsUtil {
    public static void main_t(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path("hdfs://localhost:9000/data/input/hadoop-3.0.0.tar.gz");
        FSDataInputStream in = fs.open(src);
        FileOutputStream os = new FileOutputStream("/home/wangheng/Downloads/hdfs_hadoop.tar.gz");
        IOUtils.copy(in, os);
    }
}
