package com.xuyanan.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class WordCountApp {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://10.52.58.250:8020");

        Job job = Job.getInstance(configuration);
        job.setJarByClass(WordCountApp.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://10.52.58.250:8020"), configuration, "hadoop");
        Path outputPath = new Path("/wordcount/output");
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, outputPath);
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);
    }
}
