package com.sxbdjw.data.utils.spark.streaming

import com.sxbdjw.test.{CourseClickCount, CourseClickCountDao, CourseSearchClickCount, CourseSearchClickCountDAO}
import com.sxbdjw.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable.ListBuffer

object StatStreamingApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      println("Usage:<zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, groupId, topics, numThreads) = args

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StatStreamingApp")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

    /*
    * 测试1：测试数据接收
    * */
    println("===============================================")
    messages.map(_._2).count().print()
    println("===============================================")

    val logs = messages.map(_._2)
    val cleanData = logs.map(line => {
      //infos(2) = "GET /class/128.html HTTP/1.1"
      //url =  /class/128.html
      //186.43.156.132	2019-07-29 18:42:01	"GET /class/145.html HTTP/1.1"	404	https://cn.bing.com/search?q=Storm实战
      val infos = line.split("\t")
      val url = infos(2).split(" ")(1)
      var coursetId = 0
      if (url.startsWith("/class")) {
        val courseIdHtml = url.split("/")(2)

        coursetId = courseIdHtml.substring(0, courseIdHtml.lastIndexOf(".")).toInt

      }

      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), coursetId, infos(3).toInt, infos(4))
    }).filter(clickLog => clickLog.coursetId != 0)
    cleanData.print()

    //统计到现在为止的访问量

    cleanData.map(x => {
      (x.time.substring(0, 8) + "_" + x.coursetId, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseClickCount]
        partitionRecords.foreach(pair => {
          list.append(CourseClickCount(pair._1, pair._2))
        })
        CourseClickCountDao.save(list)
      })
    })

    //统计从搜索引擎过来的访问量
    cleanData.map(x => {
      //https://search.yahoo.com/search?p=Spark SQL实战
      val referer = x.referer.replaceAll("//", "/")
      val splits = referer.split("/")
      var host = ""
      if (splits.length > 2) {
        host = splits(1)
      }
      (host, x.coursetId, x.time)
    }).filter(_._1 != "").map(x => {
      (x._3.substring(0, 8) + "_" + x._1 + "_" + x._2, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseSearchClickCount]
        partitionRecords.foreach(pair => {
          list.append(CourseSearchClickCount(pair._1, pair._2))
        })
        CourseSearchClickCountDAO.save(list)
      })
    })


  ssc.start()
  ssc.awaitTermination()
}
}
