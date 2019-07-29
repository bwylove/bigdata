package com.sxbdjw.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {

  var YYYYMMDDHHMMSS_FORMAT=FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  val TARGET_FORMAT=FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTime(time:String) ={
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }

  def parseToMinute(time:String)={
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {
    println(parseToMinute("2019-07-29 17:13:13"))
  }

}
