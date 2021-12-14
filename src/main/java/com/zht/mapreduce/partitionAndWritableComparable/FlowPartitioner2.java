package com.zht.mapreduce.partitionAndWritableComparable;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//重写序列化和反序列化方法
public class FlowPartitioner2 extends Partitioner<FlowBean,Text> {


    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {

        String phone = text.toString();
        String prePhone = phone.substring(0,3);

        int partition;
        if("136".equals(prePhone)){
            partition = 0;
        } else if("137".equals(prePhone)){
            partition = 1;
        } else if("138".equals(prePhone)){
            partition = 2;
        } else if("139".equals(prePhone)){
            partition = 3;
        } else {
            partition =  4;
        }

        return  partition;

    }
}
