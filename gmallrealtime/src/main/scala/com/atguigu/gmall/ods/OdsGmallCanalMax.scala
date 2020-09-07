package com.atguigu.gmall.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.util.{MyKafkaSender, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OdsGmallCanalMax {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("oda_gmall-max_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_DB_GMALL2020_M"
    val groupId = "oda_gmall-max_app_group"
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
    jsonObjDstream.foreachRDD{rdd=>
      rdd.foreach{jsonObj=>
        val dataJson: String = jsonObj.getString("data")
        val table: String = jsonObj.getString("table")
        val topic="ODS_MM_"+table.toUpperCase

        if(jsonObj.getString("type")!=null&&(
          (table=="order_info"&&jsonObj.getString("type")=="insert")
            || (table=="order_detail"&&jsonObj.getString("type")=="insert")
            || (table=="base_province"&&(jsonObj.getString("type")=="insert"||jsonObj.getString("type")=="update")||jsonObj.getString("type")=="bootstrap-insert")
            || (table=="user_info"&&(jsonObj.getString("type")=="insert"||jsonObj.getString("type")=="update")||jsonObj.getString("type")=="bootstrap-insert")
          )){

          MyKafkaSender.send(topic,dataJson)

        }
      }
      OffsetManager.saveOffset(topic,groupId,offsetRangs)
    }


    ssc.start()
    ssc.awaitTermination()
  }

}
