package com.zht.mapreduce.partitionAndWritableComparable;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/*
*
* KEYIN
* VALUEIN
* KEYOUT
* VALUEOUT
* */

//重写序列化和反序列化方法
public class FlowReduce extends Reducer<FlowBean,Text ,Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }


    }
}
