package com.sxbdjw.data.config;

public class Config {

    //ElasticSearch的集群名称
    public static final String CLUSTER_NAME = "bdjw_test";
    //ElasticSearch的host集合
    public static final String[] NODE_HOST={"192.168.81.21","192.168.81.22","192.168.81.23"};

    // ElasticSearch的端口（Java API用的是Transport端口，也就是TCP）
    public static final int NODE_PORT = 9300;

    // ElasticSearch的索引名称
    public static final String INDEX_NAME_PINYIN = "thyh";
    // ElasticSearch的类型名称
    public static final String TYPE_NAME = "bdjw";


    //zookeeper
    public static final String hbase_zookeeper_quorum="192.168.81.21,192.168.81.22,192.168.81.23";

    //zookeeper Port
    public  static final String hbase_zookeeper_property_clientPort="2181";



}
