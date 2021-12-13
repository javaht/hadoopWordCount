package com.zht.mapreduce.partitioner;


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
public class FlowReduce extends Reducer<Text, FlowBean,Text, FlowBean> {
    private FlowBean outV=new FlowBean();


    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

         long totalUp =0;
         long totaldown = 0;
        //遍历集合累加值
        for (FlowBean value : values) {
            totalUp +=value.getUpFlow();
            totaldown+=value.getDownFlow();
        }
        //封装outK  outV
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totaldown);
        outV.setSumFlow();


        context.write(key,outV);

    }
}
