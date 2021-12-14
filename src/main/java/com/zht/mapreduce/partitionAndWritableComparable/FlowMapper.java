package com.zht.mapreduce.partitionAndWritableComparable;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//重写序列化和反序列化方法

/*
 *
 * KEYIN
 * VALUEIN
 * KEYOUT
 * VALUEOUT
 * */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean,Text > {


    private FlowBean outK = new FlowBean();
    private Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取一行
        String line = value.toString();

        //切割
        String[] split = line.split("\t");



        outK.setUpFlow(Long.parseLong(split[1]));
        outK.setDownFlow(Long.parseLong(split[2]));
        outK.setSumFlow();
        outV.set(split[0]);



        context.write(outK,outV);

    }
}
