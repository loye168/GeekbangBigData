package com.geekbang.week3.hbase.delete;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//实现writable接口
public class DeleteData {
    /**
     * 删除数据
     *
     * @param tableName   表名
     * @param rowkey rowKeys
     * @throws IOException .
     */
    public static boolean deleteRow(Connection connection, String tableName, String rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
        System.out.println("成功删除 " + tableName + " " + rowkey);
        return true;
    }
}
