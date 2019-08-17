package com.sxbdjw.example.dao

import com.sxbdjw.data.utils.hbase.HbaseUtils
import com.sxbdjw.example.domain.CourseSearchClickCount
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object CourseSearchClickCountDAO {

  val table_name = "course_search_clickcount"
  val cf = "info"
  val qualifer = "click_count"

  def save(list: ListBuffer[CourseSearchClickCount]): Unit = {
    val table = HbaseUtils.getInstance().getTable(table_name)
    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_search_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.click_count)
    }
  }

  def count(day_search_course: String): Long = {
    val table = HbaseUtils.getInstance().getTable(table_name)
    val get = new Get(Bytes.toBytes(day_search_course))
    val value = table.get(get).getValue(cf.getBytes(), qualifer.getBytes())
    if (value==null){
      0l
    }else{Bytes.toLong(value)}
  }
}
