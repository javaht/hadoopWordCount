package com.zht.mapreduce.ReduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TableDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Job job = Job.getInstance(new Configuration());


        job.setJarByClass(TableDriver.class);
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReduce.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);


        //设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\input\\order"));
        //这个路径是父类路径  其他的继承这个
        FileOutputFormat.setOutputPath(job,new Path("D:\\output"));

        job.waitForCompletion(true);

    }
}
