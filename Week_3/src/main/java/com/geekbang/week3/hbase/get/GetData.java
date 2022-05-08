package com.geekbang.week3.hbase.get;

import com.geekbang.week3.hbase.delete.DeleteData;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;


public class GetData extends Reducer<Text, DeleteData,Text, DeleteData> {
    /**
     * 查询数据
     *
     * @param tableName   表名
     * @param connection 连接地址
     * @throws IOException .
     */
    public static void getData(Connection connection, String tableName) throws IOException {
        //Admin admin = connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        Result tmp;
        System.out.println("Row\t\t\tColumn\tvalue");
        while ((tmp = scanner.next()) != null) {
            List<Cell> cells = tmp.listCells();
            for (Cell cell : cells) {
                String rk = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String cf = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(rk + "\t\tcolumn:" + cf + ":" + column + ",value=" + value);
            }
        }
    }
}
