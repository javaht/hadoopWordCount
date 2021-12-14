package com.zht.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
* 自定义OutPutFormat总结：
* 1.首先继承FileOutputFormat 重写getRecordWriter
* 2.new一个writer继承RecordWriter 然后创建流
* */


public class LogDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job =Job.getInstance(conf);


        job.setJarByClass(LogDriver.class);

        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置自定义的logoutformat
        job.setOutputFormatClass(LogOutputFormat.class);

       //设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\input\\log"));
        //这个路径是父类路径  其他的继承这个
        FileOutputFormat.setOutputPath(job,new Path("D:\\output\\atguigu"));

     job.waitForCompletion(true);
    }
}
