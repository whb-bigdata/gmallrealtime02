package com.atguigu.gmall.util

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}
import com.alibaba.fastjson.JSONObject
import scala.collection.mutable.ListBuffer

object  PhoenixUtil {

  def main(args: Array[String]): Unit = {
    val list:  List[ JSONObject] = queryList("select * from  US_POPULATION")
    println(list)
  }

  def   queryList(sql:String):List[JSONObject]={
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val resultList: ListBuffer[JSONObject] = new  ListBuffer[ JSONObject]()
    val conn: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop202,hadoop203,hadoop204:2181")
    val stat: Statement = conn.createStatement
    println(sql)
    val rs: ResultSet = stat.executeQuery(sql )
    val md: ResultSetMetaData = rs.getMetaData
    while (  rs.next ) {
      val rowData = new JSONObject();
      for (i  <-1 to md.getColumnCount  ) {
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList+=rowData
    }

    stat.close()
    conn.close()
    resultList.toList
  }

}
