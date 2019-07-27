package com.sxbdjw.data.utils.es.dao;

import com.google.common.cache.LoadingCache;
import com.sxbdjw.data.utils.es.bulkOperator.BulkProcessorScheduler;
import com.sxbdjw.data.utils.es.conn.Connection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

public class EsClientDao {

    private final static Log log = LogFactory.getLog(EsClientDao.class);

    private LoadingCache<String, Boolean> INDEX_EXISTS;

    private LoadingCache<String, Boolean> ALAIS_EXISTS;

    private Client client;

    private BulkProcessor bulkProcessor;

    private String esConn;

    private int bulkAction;

    private int bufferSize;

    private int concurrentRequests = 1;

    private int bulkFlushInterval = 3;

    public EsClientDao(String esConn, int bulkAction, int bufferSize) {
        this.esConn = esConn;
        this.bulkAction = bulkAction;
        this.bufferSize = bufferSize;
        this.client = Connection.getTransportNodeConn();
        BulkProcessorScheduler bulkProcessorScheduler = new BulkProcessorScheduler();
    }

    public void insertOneData(String indexName, String indexType, String mapping, int shards, int replica, MsgObject msgObject) {

        boolean flag = createIndex(indexName, indexType, mapping, shards, replica);

    }

    private boolean createIndex(String indexName, String indexType, String mapping, int shards, int replica) {
        if (!indexCacheExists(indexName)) {
            log.info(String.format("索引[%s]不存在，先创建索引", indexName));
            synchronized (indexName) {
                if (!indexCacheExists(indexName)) {
                    CreateIndexResponse response = client.admin().indices()
                            .prepareCreate(indexName)
                            .setSettings(getCommonSetting(shards, replica))
                            .addMapping(indexType, mapping, XContentType.JSON)
                            .get();
                    if (response.isAcknowledged()) {
                        INDEX_EXISTS.refresh(indexName);
                        log.info("esClientDao创建索引[" + indexName + "]成功");
                    } else {
                        log.info("esClientDao创建索引[" + indexName + "]失败");
                    }
                    return response.isAcknowledged();
                }
            }
        }
        return true;
    }

    private Settings.Builder getCommonSetting(int shards, int replica) {
        return Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.number_of_replicas", replica);
    }

    private boolean indexCacheExists(String index) {
        return INDEX_EXISTS.getUnchecked(index);
    }

    public void deleteById(String indexName, String indexType, String uniId) {
        try {
            client.prepareDelete(indexName, indexType, uniId).get();
            log.info("delete odb_relation _id: [{"+uniId+"}] successful!");
        } catch (Exception e) {
            log.error("delete odb_relation _id: [{"+uniId+"}] failed!");
        }
    }

    public void destroy() {
        bulkProcessor.close();
        client.close();
    }


    public static class MsgObject {
        public String uuid;
        public String msgJsonStr;

        public MsgObject(String uuid, String msgJsonStr) {
            this.uuid = uuid;
            this.msgJsonStr = msgJsonStr;
        }
    }
}
