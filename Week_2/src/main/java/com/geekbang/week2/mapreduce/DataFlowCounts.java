package com.geekbang.week2.mapreduce;

import com.geekbang.week2.mapreduce.mapper.DataFlowMapper;
import com.geekbang.week2.mapreduce.pojo.FlowBean;
import com.geekbang.week2.mapreduce.reducer.DataFlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//测试提交
public class DataFlowCounts {
    public static void main(String[] args) throws Exception {
        System.out.println("star runing");
        args = new String[]{"/home/student1/luoye/week2/","./test.txt"};
        if (args.length != 2) {
            System.exit(-1);
        }
        //获取job信息
        Job job = Job.getInstance(new Configuration(),"flow");
        //加载jar包
        job.setJarByClass(DataFlowCounts.class);
          //关联map
        job.setMapperClass(DataFlowMapper.class);
        //关联reduce
        job.setReducerClass(DataFlowReducer.class);
        //设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //输入数据路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //输出数据路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交任务
        job.waitForCompletion(true);
    }
}
