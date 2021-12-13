package com.zht.mapreduce.partitioner;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//重写序列化和反序列化方法
public class FlowPartitioner extends Partitioner<Text,FlowBean> {


    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        //text 是手机号
        String phone = text.toString();
        String prephone = phone.substring(0, 3);
        int partition ;

        if("136".equals(prephone)){
            partition=0;
        }else if("137".equals(prephone)) {
            partition=1;

        }else if("138".equals(prephone)) {
            partition=2;

        }else if("139".equals(prephone)) {
            partition=3;

        }else{
            partition=4;
        }

        return partition;
    }
}
