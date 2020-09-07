package com.atguigu.gmall.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.bean.DauInfo
import com.atguigu.gmall.util.{MyEsUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

// dau= daily active user     uv = user view , pv = page view
object DauApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "GMALL_START"
    val groupId = "dau_app_group"
    val offset: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offset != null) {

      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    var offsetRangs: Array[OffsetRange] = null
    val value: DStream[ConsumerRecord[String, String]] = inputDstream.transform(rdd => {
      offsetRangs = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    //inputDstream.map(_.value()).print(100)

    //前置处理 1 结构化  2 日期 3 小时
    val jsonObjDstream: DStream[JSONObject] = value.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      val ts: lang.Long = jsonObj.getLong("ts")
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
      val dateTimeStr: String = dateFormat.format(new Date(ts))
      val dateTimeArr: Array[String] = dateTimeStr.split(" ")
      val dt: String = dateTimeArr(0)
      val hr: String = dateTimeArr(1)
      jsonObj.put("dt", dt)
      jsonObj.put("hr", hr)
      jsonObj
    }


    val filteredDstream: DStream[JSONObject] = jsonObjDstream.mapPartitions { jsonObjItr =>
      val jedis: Jedis = RedisUtil.getJedisClient // 1 连接池
      val jsonList: List[JSONObject] = jsonObjItr.toList
      // println("过滤前：" + jsonList.size)
      val filteredList = new ListBuffer[JSONObject]
      for (jsonObj <- jsonList) {
        val dauKey = "dau:" + jsonObj.get("dt")
        val mid = jsonObj.getJSONObject("common").getString("mid")
        val ifNonExists: lang.Long = jedis.sadd(dauKey, mid)
        if (ifNonExists == 1) {
          filteredList += jsonObj
        }
      }
      jedis.close()
      // println("过滤后：" + filteredList.size)
      filteredList.toIterator
    }


    /*    // 数据中 凡是今天启动过的用户(mid)进行记录，同时进行筛选如果之前访问过 则过滤掉
        val jsonObjFilteredDStream: DStream[JSONObject] = jsonObjDstream.filter { jsonObj =>
          // Redis  type？ string set hash zset list   key?     value
          // string   key?  dau:[date]:mid:[mid]   value 1/0    api?  setnx  如果有需求：查询今天所有的mid  keys dau:[date]:mid:*  O(N)     可以分摊到多台机器中
          // set     key ?  dau:[date]   value? [mid]    api?  sadd   如果有需求：查询今天所有的mid  smembers key O(1)  无法通过集群方式分摊数据和qps
          // hash   key ? dau:[date]   value? [mid] :1    api? hash 可以但没必要
          // zset   key?   dau:[date]   value? [mid] score  api? zadd  可以但没必要吗？
          val jedis: Jedis = RedisUtil.getJedisClient // 1 连接池
          val dauKey = "dau:" + jsonObj.get("dt")
          val mid = jsonObj.getJSONObject("common").getString("mid")
          val ifNonExists: lang.Long = jedis.sadd(dauKey, mid)
          jedis.close()
          if (ifNonExists == 1) {
            true
          } else {
            false
          }

        }*/

    //filteredDstream.print(100)

    filteredDstream.foreachRDD {
      rdd =>
        //保存数据,1不幂等2每条操作
        /*        rdd.foreach { startupJsonObj =>
                  val commonJSONObj: JSONObject = startupJsonObj.getJSONObject("common")
                  val dauInfo: DauInfo = DauInfo(commonJSONObj.getString("mid"), commonJSONObj.getString("uid"), commonJSONObj.getString("mid"), commonJSONObj.getString("ch")
                    , commonJSONObj.getString("vc"), commonJSONObj.getString("dt"), commonJSONObj.getString("hr"), "00", startupJsonObj.getLong("ts"))
                MyEsUtil.saveDoc(dauInfo,"gmall_dau_info" + commonJSONObj.getString("dt") )
                }*/
        //保存数据1幂等2批次进行操作
        rdd.foreachPartition { startupJsonItr =>


          val list: List[JSONObject] = startupJsonItr.toList
          if (list.size > 0) {
            val docList: List[(DauInfo, String)] = list.map { jsonObj =>
              val commonJSONObj: JSONObject = jsonObj.getJSONObject("common")
              val dauInfo: DauInfo = DauInfo(commonJSONObj.getString("mid"), commonJSONObj.getString("uid"), commonJSONObj.getString("mid"), commonJSONObj.getString("ch")
                , commonJSONObj.getString("vc"), jsonObj.getString("dt"), jsonObj.getString("hr"), "00", jsonObj.getLong("ts"))

              (dauInfo, dauInfo.mid)

            }
            val dt: String = docList(0)._1.dt
           // println( docList(0))
            MyEsUtil.saveDocBulk(docList, "gmall_dau_info" + docList(0)._1.dt)
          }
        }
        //提交offset
        OffsetManager.saveOffset(topic, groupId, offsetRangs)
    }


    ssc.start()
    ssc.awaitTermination()


  }

}
