package com.geekbang.week2.mapreduce.reducer;

import com.geekbang.week2.mapreduce.pojo.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DataFlowReducer extends Reducer<Text, FlowBean,Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //定义计数器，计算每个用户上传和下载的流量
        long sumUpFlow = 0;
        long sumDownFolw = 0;
        //累加的用户流量和
        for (FlowBean flowBean : values){
            sumUpFlow+=flowBean.getUpflow();
            sumDownFolw+= flowBean.getDownflow();
        }
        //输出
        context.write(key,new FlowBean(sumUpFlow,sumDownFolw));
    }
}
