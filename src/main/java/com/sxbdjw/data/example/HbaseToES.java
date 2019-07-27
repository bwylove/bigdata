package com.sxbdjw.data.example;


import com.sxbdjw.data.utils.es.dao.EsClientDao;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;


public class HbaseToES extends BaseRegionObserver {

    private static final Log log = LogFactory.getLog(HbaseToES.class);

    private EsClientDao esClientDao;
    private String mapping;
    private String indexName;
    private String indexType;
    private String[] indexFieldArr;
    private int indexShards;
    private int indexReplica;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
//        super.start(e);
        Configuration configuration = env.getConfiguration();
        this.indexName = configuration.get("index.name");
        this.indexType = this.indexType + "_type";
        indexFieldArr = configuration.get("index.fields").split("&");
        this.mapping = buildMapping();
        this.indexShards = configuration.getInt("index.shards", 5);
        this.indexReplica = configuration.getInt("index.replica", 11);
        int batchSize = configuration.getInt("index.batch.size", 1000);
        int bufferSize = configuration.getInt("index.buffer.size", 5);
        String esUrl = configuration.get("es.url");
        this.esClientDao = new EsClientDao(esUrl, batchSize, bufferSize);
    }

    private String buildMapping() {
        StringBuilder builder = new StringBuilder().append("{\"" + indexType + "\":{\"properties\":{");
        for (String temp : indexFieldArr) {
            String[] fieldArr = temp.split(":");
            builder.append("\"" + fieldArr[0] + "|" + fieldArr[1] + "\":{\"type\":\"" + fieldArr[2] + "\"},");
        }
        builder.setLength(builder.length() - 1);
        builder.append("}}}");
        return builder.toString();
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> env, Put put, WALEdit edit, Durability durability) throws IOException {
//        super.postPut(e, put, edit, durability);

        String newRowKey = Bytes.toString(put.getRow());
        String dataStrJson = buildPutMsg(put);
        EsClientDao.MsgObject msgObject = new EsClientDao.MsgObject(newRowKey, dataStrJson);
        esClientDao.insertOneData(indexName,indexType,mapping,indexShards,indexReplica,msgObject);

    }

    private String buildPutMsg(Put put) {
        HashMap<String, Object> hashMap = new HashMap<>();
        for (String temp : indexFieldArr) {
            String[] fieldArr = temp.split(":");
            List<Cell> cells = put.get(fieldArr[0].getBytes(), fieldArr[1].getBytes());
            if (cells != null && cells.size() > 0) {
                Cell cell = cells.get(0);
                String value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                hashMap.put(temp, value);
            }
        }
        return JSONObject.toJSONString(hashMap);
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> env, Delete delete, WALEdit edit, Durability durability) throws IOException {
//        super.postDelete(e, delete, edit, durability);
        esClientDao.deleteById(indexName,indexType,delete.getId());
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
//        super.stop(e);
        esClientDao.destroy();
    }
}
