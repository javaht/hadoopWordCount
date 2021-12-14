package com.zht.mapreduce.writableComparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//这个是全排序
//继承WritableComparable的compareTo方法进行排序
public class FlowBean implements WritableComparable<FlowBean> {
    private long upFlow; //上行流量

    private long downFlow;

    private long sumFlow;//总流量

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }


    //这个是空参构造
    public FlowBean() {

    }

    //序列化方法
    public void write(DataOutput out) throws IOException {

        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);

    }

    public void readFields(DataInput in) throws IOException {

        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public int compareTo(FlowBean o) {
        //总流量的倒叙
        if(this.sumFlow>o.sumFlow){
            return -1;
            //-1是倒叙
        }else if(this.sumFlow<o.sumFlow){
            return 1;
        }else{
            return  0;
        }
    }
}
