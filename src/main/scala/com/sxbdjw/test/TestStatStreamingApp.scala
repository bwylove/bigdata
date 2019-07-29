//package com.sxbdjw.test
//
//import com.typesafe.config.ConfigFactory
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import scalikejdbc._
//import kafka.common.TopicAndPartition
//
//
//object SetupJdbc{
//  def apply(driver:String,host:String,user:String,password:String): Unit={
//    Class.forName(driver)
//    ConnectionPool.singleton(host,user,password)
//  }
//}
//
//object TestStatStreamingApp {
//
//  def setupSsc(
//                kafkaParams: Map[String, String],
//                jdbcDriver: String,
//                jdbcUrl: String,
//                jdbcUser: String,
//                jdbcPassword: String,
//                topic: String, group: String)():StreamingContext={
//    val conf=new SparkConf()
//      .setMaster("local[2]")
//      .setAppName("offset")
//
//    val ssc=new StreamingContext(conf,Seconds(5))
//    SetupJdbc(jdbcDriver,jdbcUrl,jdbcUser,jdbcPassword)
//
//    val fromOffsets=DB.readOnly { implicit session =>
//      sql"select topic, part, offset from streaming_task where group_id=$group".
//        map { resultSet =>
//          new TopicAndPartition(resultSet.string(1), resultSet.string(2) -> resultSet.long(3))
//        }.list.apply().toMap}
//
//  def main(args: Array[String]): Unit = {
//    val conf = ConfigFactory.load()
//    val kafkaParams = Map[String, String](
//      "metadata.broker.list" -> conf.getString("kafka.brokers"),
//      "group.id" -> conf.getString("kafka.group"),
//      "auto.offset.reset" -> "smallest"
//    )
//    val jdbcDriver = conf.getString("jdbc.driver")
//    val jdbcUrl = conf.getString("jdbc.url")
//    val jdbcUser = conf.getString("jdbc.user")
//    val jdbcPassword = conf.getString("jdbc.password")
//
//    val topic = conf.getString("kafka.topics")
//    val group = conf.getString("kafka.group")
//
//    val ssc=setupSsc(kafkaParams,jdbcDriver,jdbcUrl,jdbcUser,jdbcPassword,topic,group)()
//
//  }
//}
//
//
