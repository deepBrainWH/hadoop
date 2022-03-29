package com.wangheng.hadoop.join.map;

import com.wangheng.hadoop.join.TableBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jline.utils.InputStreamReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wangheng
 * @date 2021/12/20
 * @类职责
 * @设计文档
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, TableBean, NullWritable> {
  private final Map<Long, String> cachedPdc = new HashMap<>();

  @Override
  protected void setup(Context context) throws IOException {
    //获取缓存文件,并把内容封装到集合
    URI[] cacheFiles = context.getCacheFiles();
    FileSystem fileSystem = FileSystem.get(context.getConfiguration());
    for (URI uri : cacheFiles) {
      FSDataInputStream inputStream = fileSystem.open(new Path(uri));
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
      String line;
      while (StringUtils.isNotEmpty(line = bufferedReader.readLine())) {
        String[] split = line.split("\\s+");
        long pid = Long.parseLong(split[0]);
        String name = split[1];
        cachedPdc.put(pid, name);
      }
      bufferedReader.close();
      inputStream.close();
    }
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] split = value.toString().split("\\s+");
    TableBean bean = new TableBean(Integer.parseInt(split[0]), Integer.parseInt(split[1]), Integer.parseInt(split[2]), cachedPdc.get(Long.parseLong(split[1])), null);
    context.write(bean, NullWritable.get());
  }
}
