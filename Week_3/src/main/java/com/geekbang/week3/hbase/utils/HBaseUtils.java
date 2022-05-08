package com.geekbang.week3.hbase.utils;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HBaseUtils {
    private static Connection CONNECTION;
    private static final Logger logger = LoggerFactory.getLogger(HBaseUtils.class);
    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "114.55.52.33");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try {
            CONNECTION = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Connection createConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.0.197");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        return ConnectionFactory.createConnection(conf);
    }

    public static void main(String[] args) {
        try {
            Connection CONNECTION = HBaseUtils.createConnection();
            if ( CONNECTION != null){
                logger.info("连接成功 {}",CONNECTION);
            }
            System.out.println("连接失败 {}");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
