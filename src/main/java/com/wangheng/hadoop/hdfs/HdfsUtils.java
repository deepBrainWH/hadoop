package com.wangheng.hadoop.hdfs;

import com.jcraft.jsch.IO;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class HdfsUtils {
    FileSystem fs = null;

    @Before
    public void init() throws Exception{
        //read the xxx-site.xml properties file in classpath and get theris content
        //and finally get an object about properties file
        Configuration conf = new Configuration();
        //this will recover the xxx-site.xml file's properties
//        conf.set("fs.defaultFS", "hdfs://localhost:9000/");
        fs = FileSystem.get(new URI("hdfs://localhost:9000/"), conf, "wangheng");
    }
    /**
     * upload file
     * 较低层的文件上传方法
     */
    @Test
    public void upload() throws Exception{
        Path dst = new Path("hdfs://localhost:9000/data/input/storm1.pdf");
        FSDataOutputStream os = fs.create(dst);

        FileInputStream is = new FileInputStream("/home/wangheng/Documents/storm.pdf");
        IOUtils.copy(is, os);
    }

    /**
     * easy upload file to hdfs file system
     * advance method to upload file from local
     */
    @Test
    public void easyUpload() throws Exception{
        fs.copyFromLocalFile(new Path("/home/wangheng/Documents/storm.pdf"),
                new Path("hdfs://localhost:9000/data/wangheng/java/storm4.pdf"));
    }

    /**
     * download file from hdfs file system
     */
    @Test
    public void download() throws IOException {
        fs.copyToLocalFile(new Path("/data/input/storm1.pdf"),
                new Path("/home/wangheng/storm.pdf"));

    }

    /**
     * mkdir
     */
    @Test
    public void mkdir() throws IOException{
        fs.mkdirs(new Path("/data/wangheng/java"));
    }

    /**
     * list files
     */
    @Test
    public void listFiles() throws IOException {
        //listFiles 列出的是文件信息，而且提供递归
        RemoteIterator<LocatedFileStatus> filse = fs.listFiles(new Path("/"), true);
        while (filse.hasNext()){
            LocatedFileStatus file = filse.next();
            Path filePath = file.getPath();
            String filename = filePath.getName();
            System.out.println(filePath.toString()+"\t"+filename);
        }

        System.out.println("------------------------------------");
        // listStatus 可以列出文件夹信息，但是不提供递归便利

        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus status: listStatus){
            String filename = status.getPath().getName();
            System.out.println((status.isDirectory()?"DIR":"file")+"\t"+filename);
        }

    }

    /**
     * delete file
     */
    @Test
    public void deleteFile() throws IOException{
        fs.delete(new Path("/data/input/"), true);
    }
}
