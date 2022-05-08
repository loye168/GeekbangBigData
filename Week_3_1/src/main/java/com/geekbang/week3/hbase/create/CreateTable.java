package com.geekbang.week3.hbase.create;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;


import java.io.IOException;
import java.util.Arrays;

//分割文件
public class CreateTable{
        /**
         * 创建表
         *
         * @param tableName      表名
         * @param columnFamilies 列族名
         * @throws IOException .
         */
        public static void create(Connection connection, String tableName, String... columnFamilies) throws IOException {
            Admin admin = connection.getAdmin();
            if (tableName == null || columnFamilies == null) {
                return;
            }
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
            for (int i = 0; i < columnFamilies.length; i++) {
                if (columnFamilies[i] == null)
                    continue;
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamilies[i]);
                columnDescriptor.setMaxVersions(1);
                table.addFamily(columnDescriptor);
            }
            admin.createTable(table);
            System.out.println("成功创建表 " + table + ", column family: " + Arrays.toString(columnFamilies));
        }
    //创建表空间 TableNameSpace:TableName--> xiaoyuer:student
    private static class CreateNameSpace {
        public  void createNamespace(Connection connection, String tablespace) throws IOException {
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            admin.createNamespace(NamespaceDescriptor.create(tablespace).build());
            System.out.println("成功创建表空间 " + tablespace);
        }
    }
}
