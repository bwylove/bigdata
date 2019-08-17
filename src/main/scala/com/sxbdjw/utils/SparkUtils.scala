package com.sxbdjw.utils

import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class SparkUtils {

  private[this] val options = new mutable.HashMap[String, String]()

  val sparkConf: SparkConf = new SparkConf()
  var sc: SparkContext = _

  def config(key: String, value: String): SparkUtils = synchronized {
    options += key -> value
    this
  }

  def getConf: SparkConf = {
    sparkConf
  }

  def appName(name: String): SparkUtils = {
    sparkConf.setAppName(name)
    this
  }

  def setMaster(name: String): SparkUtils = {
    val osName = System.getProperties.getProperty("os.name")
    println(s"OS NAME:[$osName]")
    if (osName != "Linux") sparkConf.setMaster(name)
    this
  }

  def getSparkContext: SparkContext = {
    options.foreach {
      case (k, v) => sparkConf.set(k, v)
    }
    new SparkContext(sparkConf)
  }

  def getSparkStreaming(batchDuration: String, batchType: String = "Seconds"): StreamingContext = {
    options.foreach {
      case (k, v) => sparkConf.set(k, v)
    }
    val batchDurationSuffix = batchDuration.last
    val batchTime = batchDuration.split(batchDurationSuffix)(0).toInt
    if (batchDurationSuffix == "m") {
      val ssc = new StreamingContext(sparkConf, Minutes(batchTime))
      this.sc = ssc.sparkContext
      sc.setLogLevel("WARN")
      ssc
    } else {
      val ssc = new StreamingContext(sparkConf, Seconds(batchTime))
      this.sc = ssc.sparkContext
      ssc
    }
  }

  def startStreaming(ssc: StreamingContext): Unit = {
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
