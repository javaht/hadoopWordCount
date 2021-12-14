package com.zht.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/*
*
* KEYIN       reduce阶段输入的key类型        Text
* VALUEIN       reduce阶段输入的value类型     Int
* KEYOUT        reduce阶段输出的key类型       Text
* VALUEOUT         reduce阶段输出的value类型   Int
* */
public class WordCountReduce extends Reducer<Text, IntWritable,Text, IntWritable> {
    IntWritable outV = new IntWritable();
    @Override
    //都是输入的key 和values
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {
            sum +=value.get();
        }
        outV.set(sum);
        context.write(key,outV);
    }
}
