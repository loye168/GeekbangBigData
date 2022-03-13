package com.geekbang.week2.mapreduce.pojo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//实现writable接口
public class FlowBean implements Writable {
    //上行流量
    private long upflow;
    //下行流量
    private long downflow;
    //总流量
    private long sumflow;

    public long getUpflow() {
        return upflow;
    }

    public void setUpflow(long upflow) {
        this.upflow = upflow;
    }

    public long getDownflow() {
        return downflow;
    }

    public void setDownflow(long downflow) {
        this.downflow = downflow;
    }

    public long getSumflow() {
        return sumflow;
    }

    public void setSumflow(long sumflow) {
        this.sumflow = sumflow;
    }

    //构造器
    public FlowBean(long upflow, long downflow) {
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow = upflow + downflow;
    }

    //序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.upflow);
        dataOutput.writeLong(this.downflow);
        dataOutput.writeLong(this.sumflow);
    }

    //反序列化，
    public void readFields(DataInput dataInput) throws IOException {
        this.upflow = dataInput.readLong();
        this.downflow = dataInput.readLong();
        this.sumflow = dataInput.readLong();
    }

    //编写toString方法，方便后续打印到文本，注意格式
    @Override
    public String toString() {
        return this.upflow + "\t" + this.downflow + "\t" + this.sumflow;
    }
}
