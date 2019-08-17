package com.sxbdjw.data.utils.es.bulkOperator;

import com.sxbdjw.data.utils.es.conn.Connection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BulkProcessorScheduler {

    private static final Log log = LogFactory.getLog(BulkProcessorScheduler.class);

    private final Lock commitBulkLock = new ReentrantLock();

    private Client client;

    private BulkProcessor bulkProcessor;

    public BulkProcessorScheduler build() {
        this.client = Connection.getTransportNodeConn();
        this.bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            public void beforeBulk(long paramLong, BulkRequest paramBulkRequest) {
                log.info("----start sync data: " + paramBulkRequest.numberOfActions() + "num of data----");
            }

            public void afterBulk(long paramLong, BulkRequest paramBulkRequest, BulkResponse paramBulkResponse) {
                log.info("----end sync data" + paramBulkRequest.numberOfActions() + "num of data success----");
            }

            //执行出错时
            public void afterBulk(long paramLong, BulkRequest paramBulkRequest, Throwable paramThrowable) {
                log.info("----fail sync data: " + paramBulkRequest.numberOfActions() + "num of data,the reason is " + paramThrowable.getMessage());
                log.info("----retry to sync " + paramBulkRequest.numberOfActions() + "data start----");
                Connection.getTransportNodeConn().prepareBulk().request().add(paramBulkRequest.requests());
                log.info("----retry to sync" + paramBulkRequest.numberOfActions() + "data end----");
            }
        })
                //1w次请求一次bulk
                .setBulkActions(10000)
                //1GB的数据刷新一次bulk
//                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                //固定必须50s刷新一次
                .setFlushInterval(TimeValue.timeValueSeconds(50))
                //并发请求数量，0不并发，1并发允许执行
                .setConcurrentRequests(1)
                //设置退避，100ms后执行，最大请求3次
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 10))
                .build();
        return this;
    }

    public void close() {
        this.client.close();
        this.bulkProcessor.close();
    }
//删除操作
    public BulkProcessorScheduler add(DeleteRequest deleteRequest) {
        commitBulkLock.lock();
        try {
            this.client = Connection.getTransportNodeConn();
            this.bulkProcessor.add(deleteRequest);
            return this;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            commitBulkLock.unlock();
        }
        return this;
    }
    //修改
    public BulkProcessorScheduler add(UpdateRequest updateRequest){
        commitBulkLock.lock();
        try {
            this.client = Connection.getTransportNodeConn();
            this.bulkProcessor.add(updateRequest);
            return this;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            commitBulkLock.unlock();
        }
        return this;
    }



}
