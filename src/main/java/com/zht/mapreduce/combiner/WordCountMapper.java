package com.zht.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
* KEYIN   map阶段输入的key类型            偏移量的类型LongWritable
* VALUEIN  map阶段输入的value类型                   Text
* KEYOUT    map阶段输出的key类型                    Text(单词类型)
* VALUEOUT   map阶段输出的value类型               IntWritable(单词的个数)
 *  */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

   private  Text outK = new Text();
   private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         //获取一行
        String[] words = value.toString().split(" ");

        for (String word : words) {
            //封装outK
            outK.set(word);
            //写出
            context.write(outK,outV);
        }
    }
}
