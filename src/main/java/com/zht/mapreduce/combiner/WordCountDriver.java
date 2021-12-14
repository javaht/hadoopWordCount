package com.zht.mapreduce.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WordCountDriver {
//combiner用的时候判断会不会影响最终的逻辑   直接 用自身的   job.setCombinerClass(WordCountReduce.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        //获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置jar包路径
        job.setJarByClass(WordCountDriver.class);

        //关联mapper和reduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        //设置map的输出的k  v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置最终输出的k v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //combiner  不用写 因为conbiner和reduce一样
        job.setCombinerClass(WordCountReduce.class);

        //设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\output1"));




        //提交job
        boolean result = job.waitForCompletion(true);

        System.out.println(result ? 0 : 1);

    }
}
