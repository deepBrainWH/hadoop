package com.wangheng.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;

public class WriteToHDFS {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000/");
        try {
            FileSystem fileSystem = FileSystem.get(URI.create("hdfs://localhost:9000"), conf, "wangheng");
            LocalFileSystem localFileSystem = FileSystem.getLocal(conf);
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
            for (FileStatus fileStatuse : fileStatuses) {
                System.out.println( fileStatuse.getPath().toString() +
                        "\t\t\t\t\t\t\t" + "\033[32m" +(fileStatuse.isDirectory()?"DIR":"FILE") + "\033[0m");
            }
            FileStatus[] localFileStatus = localFileSystem.listStatus(new Path("/"));
            for(FileStatus fileStatus: localFileStatus){
                System.out.println( fileStatus.getPath().toString() +
                        "\t\t\t\t\t\t\t" + "\033[32m" +(fileStatus.isDirectory()?"DIR":"FILE") + "\033[0m");
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
