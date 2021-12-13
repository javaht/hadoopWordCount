package com.zht.mapreduce.partitioner;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//重写序列化和反序列化方法
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


         //获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        //设置jar包
        job.setJarByClass(FlowDriver.class);

        //关联mapper和reduce
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);

        //设置mapper输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);


        //设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        //设置自定义的partition
        job.setPartitionerClass(FlowPartitioner.class);
        job.setNumReduceTasks(5);


        //设置数据的输入和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\input\\phone"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\output"));

        //提交job
        job.waitForCompletion(true);




    }
}
