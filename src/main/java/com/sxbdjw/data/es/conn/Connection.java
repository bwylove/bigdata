package com.sxbdjw.data.es.conn;

import com.sxbdjw.data.config.Config;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

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
}
