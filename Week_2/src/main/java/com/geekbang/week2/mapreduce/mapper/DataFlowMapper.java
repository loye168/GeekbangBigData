package com.geekbang.week2.mapreduce.mapper;

import com.geekbang.week2.mapreduce.pojo.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//分割文件
public class DataFlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取数据
        String line = value.toString();
        //切分数据
        String[] fields = line.split("\t");
        //**获取**上传流量
        //从后往前数,转换成long
        long upflow = Long.parseLong(fields[fields.length - 3]);
        long downflow = Long.parseLong(fields[fields.length - 2]);
        //输出   Text是用户     value是上下行流量
        context.write(new Text(fields[1]) , new FlowBean(upflow,downflow));
    }
}
