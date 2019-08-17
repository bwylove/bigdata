package com.sxbdjw.data.observer;

import com.sxbdjw.data.utils.es.bulkOperator.BulkProcessorScheduler;
import com.sxbdjw.data.utils.es.conn.Connection;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class TestObserver extends BaseRegionObserver {

    private Client client;
    private BulkProcessorScheduler bulkProcessorScheduler;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        this.client = Connection.getTransportNodeConn();
        this.bulkProcessorScheduler = new BulkProcessorScheduler();
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        bulkProcessorScheduler.close();
        client.close();
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
       String indexId=new String(put.getRow());
        NavigableMap<byte[], List<Cell>> familyCellMap = put.getFamilyCellMap();
        Map<String, Object> infoJson = new HashMap<>();
        Map<String, Object> json = new HashMap<>();
        for (Map.Entry<byte[],List<Cell>> entry:familyCellMap.entrySet()){
            for (Cell cell:entry.getValue()){
                String key= Bytes.toString(CellUtil.cloneQualifier(cell));
                String value=Bytes.toString(CellUtil.cloneValue(cell));
                json.put(key,value);
            }
        }
        infoJson.put("info",infoJson);
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

    }
}
