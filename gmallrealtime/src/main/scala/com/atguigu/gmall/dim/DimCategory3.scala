package com.atguigu.gmall.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.bean.Category3
import com.atguigu.gmall.util.{MyKafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.phoenix.spark._
object DimCategory3 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dim_base_category3_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_MM_BASE_CATEGORY3"
    val groupId = "dim_base_category3_group"
    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    val offsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)

    //2、 把偏移量交给kafka ，让kafka按照偏移量的位置读取数据流
    if (offsetMap != null) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //3、  获得偏移量的结束位置
    //从流中rdd 获得偏移量的结束点 数组
    var offsetRanges: Array[OffsetRange] = null
    val inputWithOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    val jsonObjDstream: DStream[JSONObject] = inputWithOffsetDstream.map { record =>
      val jsonString: String = record.value()
      JSON.parseObject(jsonString)
    }


    //写入hbase (phoenix)
    jsonObjDstream.foreachRDD { rdd =>
      // xxxx driver
      val skuRDD: RDD[Category3] = rdd.map { jsonObj => //ex
        val category3: Category3 = Category3(jsonObj.getString("id"),
          jsonObj.getString("name"),
          jsonObj.getString("category2_id")
        )
        println(category3)

        category3
      }
      //保存到HBase
      skuRDD.saveToPhoenix("GMALL_BASE_CATEGORY3",
        Seq("ID", "NAME", "CATEGORY2ID"), new Configuration,
        Some("hadoop202,hadoop203,hadoop203:2181")
      )

      //提交偏移量
      OffsetManager.saveOffset(topic, groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
