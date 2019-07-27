package com.sxbdjw.data.utils.hbase;

import com.sxbdjw.data.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseUtils {

    HBaseAdmin admin = null;

    Configuration configuration = null;

    private HbaseUtils() {
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", Config.hbase_zookeeper_quorum);
        configuration.set("hbase.zookeeper.property.clientPort", Config.hbase_zookeeper_property_clientPort);
        configuration.set("hbase.rootdir", "hdfs://bigdata:8020/hbase");

        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HbaseUtils instance = null;

    public static synchronized HbaseUtils getInstance() {
        if (null == instance) {
            instance = new HbaseUtils();
        }
        return instance;
    }

    public HTable getTable(String tableName) {
        HTable table = null;
        try {
            table = new HTable(configuration, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public void put(String tableName,String rowKey,String cf,String column,String value){
        HTable table=getTable(tableName);
        Put put=new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
