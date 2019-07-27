package com.sxbdjw.test

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object TestStatStreamingApp {

  def main(args: Array[String]): Unit = {
    if (args.length<4){
      println("Usage:<zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum,groupId,topics,numThreads)=args

    val sparkConf=new SparkConf().setAppName("StatStreamingApp").setMaster("local[2]")
//
    val ssc=new StreamingContext(sparkConf,Seconds(60))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "bigdata:9092,bigdata02:9092,bigdata03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "cloudera_mirrormaker",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
//
//    val messages=  KafkaUtils.createDirectStrea


  }

}
