package com.geekbang.week3.hbase.put;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PutData {
    private static final Logger logger = LoggerFactory.getLogger(PutData.class);
    /**
     * 添加数据
     *
     * @param tableName 表名
     * @param columnFamily 列族名
     * @throws IOException .
     */
    public static void insert(Connection connection, String tableName, String rowKey, String columnFamily, String column,
                              String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes());
        table.put(put);
    }
}
