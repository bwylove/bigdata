package com.sxbdjw.data.utils.es.bulkOperator;

import com.sxbdjw.data.utils.es.conn.Connection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BulkScheduler {
    private static final Log log = LogFactory.getLog(BulkScheduler.class);
    private BulkRequestBuilder bulkRequestBuilder;
    private Client client;
    private int bulkSize;
    private long freshInterval;
    private final Lock commitLock = new ReentrantLock();
    private long failTimes = 0L;

    public BulkScheduler() {
        this.client = client;
    }

    public BulkScheduler build() {
        bulkRequestBuilder = client.prepareBulk();
        Timer timer = new Timer();
        timer.schedule(new CommitTimer(), 10 * 1000, freshInterval);
        return this;
    }

    public BulkScheduler setBulkSize() {
        this.bulkSize = bulkSize;
        return this;
    }

    public BulkScheduler setFreshInterval(Long freshInterval) {
        this.freshInterval = freshInterval;
        return this;
    }

    public BulkScheduler add(DeleteRequest request) {
        try {
            bulkRequestBuilder.add(request);
            bulkRequest(this.bulkSize);
            return this;
        }catch (Exception e){
            e.printStackTrace();
        }
        return this;
    }
    public BulkScheduler add(IndexRequest request) {
        try {
            bulkRequestBuilder.add(request);
            bulkRequest(this.bulkSize);
            return this;
        }catch (Exception e){
            e.printStackTrace();
        }
        return this;
    }
    public BulkScheduler add(UpdateRequest request) {
        try {
            bulkRequestBuilder.add(request);
            bulkRequest(this.bulkSize);
            return this;
        }catch (Exception e){
            e.printStackTrace();
        }
        return this;
    }


    /**
     * 定时任务，避免RegionServer迟迟无数据更新，导致ElasticSearch没有与HBase同步
     */
    private class CommitTimer extends TimerTask {
        @Override
        public void run() {
            try {
                log.info("bulk flush is running");
                bulkRequest(0);
                log.info("bulk flush is end");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 判断缓存池是否已满，批量提交
     *
     * @param threshold
     */
    private void bulkRequest(int threshold) {
        commitLock.lock();
        try {
            if (bulkRequestBuilder.numberOfActions() > threshold) {
                client = Connection.getTransportNodeConn();
                BulkResponse bulkResponses = bulkRequestBuilder.get();
                if (!bulkResponses.hasFailures()) {
                    log.info("the es bulk success: " + bulkResponses.toString());
                    bulkRequestBuilder = Connection.getTransportNodeConn().prepareBulk();
                } else {
                    for (BulkItemResponse bulkItemResponse : bulkResponses) {
                        if (bulkItemResponse.isFailed()) {
                            BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                            log.error("the es bulk error times:" + failTimes + " error reason " + failure.getMessage());
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            commitLock.unlock();
        }
    }
}
