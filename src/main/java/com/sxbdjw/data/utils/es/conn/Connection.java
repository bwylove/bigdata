package com.sxbdjw.data.utils.es.conn;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sxbdjw.data.config.Config;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Connection {

    private static final Log log = LogFactory.getLog(Connection.class);
    private static volatile TransportClient client = null;

    public static TransportClient getTransportNodeConn() {
        if (client == null || client.connectedNodes().size() <= 0) {
            synchronized (Connection.class) {
                if (client == null || client.connectedNodes().size() <= 0) {
                    log.info("es create connection");
                    Settings settings = Settings.builder()
                            .put("cluster.name", Config.CLUSTER_NAME)
                            .put("client.transport.sniff", true).build();
                    try {
                        TransportAddress[] transportAddresses = new TransportAddress[Config.NODE_HOST.length];
                        for (int i = 0; i < Config.NODE_HOST.length; i++) {
                            transportAddresses[i] = new TransportAddress(InetAddress.getByName(Config.NODE_HOST[i]), Config.NODE_PORT);
                        }
                        client = new PreBuiltTransportClient(settings).addTransportAddresses(transportAddresses);
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                        log.error("get connection to es fail");
                    }
                }
            }
        }
        return client;
    }
//测试
//    public static void main(String[] args) {
//        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//        List list = new ArrayList<>();
//        list.add("N181023194429296474");
//        boolQueryBuilder.must(QueryBuilders.nestedQuery("order_item", QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("order_item.item_order_no", list)), ScoreMode.None));
//        SearchResponse response = getTransportNodeConn().prepareSearch("t_order_address").setQuery(boolQueryBuilder).get();
//        Map<String, String> map = new HashMap<>();
//        if (response.getHits().getHits().length == 0) {
//            return;
//        }
//        for (SearchHit hits : response.getHits().getHits()) {
//            System.out.println(hits.getSourceAsString());
//            JSONObject orderItem = JSONObject.parseObject(hits.getSourceAsString());
//            JSONArray orderArray = JSONObject.parseArray(orderItem.get("order_item").toString());
//            for (int i = 0; i < orderArray.size(); i++) {
//                JSONObject parse = JSONObject.parseObject(orderArray.get(i).toString());
//                map.put(parse.get("item_order_no").toString() + "_" + parse.get("item_sku").toString(), parse.toJSONString());
//            }
//        }
//        for (String m : map.keySet()) {
//            System.out.println(map.get(m));
//        }
//    }


}
