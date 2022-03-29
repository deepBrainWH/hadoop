package com.wangheng.hadoop.join.map;

import com.wangheng.hadoop.join.TableBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author wangheng
 * @date 2021/12/20
 * @类职责
 * @设计文档
 */
public class MapJoinRunner {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.addCacheFile(new URI("file:///Users/wangheng/workspace/bigdata/hadoop/src/main/resources/test_data/input/pd.txt"));

        job.setJarByClass(MapJoinRunner.class);
        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(TableBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);

        Path inputPath = new Path("/Users/wangheng/workspace/bigdata/hadoop/src/main/resources/test_data/input/order.txt");
        Path outputPath = new Path("/Users/wangheng/workspace/bigdata/hadoop/src/main/resources/test_output/map_join");
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }
}
