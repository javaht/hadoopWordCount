package com.zht.mapreduce.Etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EtlMapper extends Mapper<LongWritable, Text,Text, Writable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //194.237.142.21 - - [18/Sep/2013:06:49:18 +0000] "GET /wp-content/uploads/2013/07/rstudio-git3.png HTTP/1.1" 304 0 "-" "Mozilla/4.0 (compatible;)"
        //获取一行

        String line = value.toString();
        //切割



        //ETL清洗
           boolean  result = parseLog(line,context);

           if(!result){
               return;
           }
           context.write(value, NullWritable.get());

    }

    private boolean parseLog(String line, Context context) {
        String[] fields = line.split(" ");
        //判断日志的长度是否大于11
        if(fields.length>11){
            return true;
        }else{
            return false;
        }
    }
}
