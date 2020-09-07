package com.atguigu.gmall.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.alibaba.fastjson.JSONArray
import com.atguigu.gmall.util.{MyKafkaSender, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object OdsGmallCanal {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("oda_gmall-canal_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "GMALL2020_DB"
    val groupId = "oda_gmall-canal_app_group"
    val offset: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offset != null) {

      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }


    var offsetRangs: Array[OffsetRange] = null
    val inputWithOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform(rdd => {
      offsetRangs = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })


    val jsonObjDstream: DStream[JSONObject] = inputWithOffsetDstream.map { record =>
      val jsonString: String = record.value()
      JSON.parseObject(jsonString)
    }

    // 数据筛选 ， 分流
    jsonObjDstream.foreachRDD { rdd =>
      rdd.foreach { jsonObj =>
        val jSONArray: JSONArray = jsonObj.getJSONArray("data")
        val table: String = jsonObj.getString("table")
        val topic = "ODS_MN_" + table.toUpperCase
        for (i <- 0 to jSONArray.size() - 1) {
          val dataJson: String = jSONArray.getString(i)
          MyKafkaSender.send(topic, dataJson)
        }
      }
      OffsetManager.saveOffset(topic, groupId, offsetRangs)
    }


    ssc.start()
    ssc.awaitTermination()
  }

}
