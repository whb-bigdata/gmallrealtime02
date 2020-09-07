package com.atguigu.gmall.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.bean.{ProvinceInfo, SkuInfo}
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
object DimSkuInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dim_sku_info_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_MM_SKU_INFO"
    val groupId = "dim_sku_info_group"
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
      val skuRDD: RDD[SkuInfo] = rdd.map { jsonObj => //ex
        val skuInfo: SkuInfo = SkuInfo(jsonObj.getString("id"),
          jsonObj.getString("spu_id"),
          jsonObj.getString("tm_id"),
          jsonObj.getString("category3_id"))
        println(skuInfo)

        skuInfo
      }
      //保存到HBase
      skuRDD.saveToPhoenix("GMALL_SKU_INFO",
        Seq("ID", "SPUID", "TMID", "CATEGORY3ID"), new Configuration,
        Some("hadoop202,hadoop203,hadoop203:2181")
      )

      //提交偏移量
      OffsetManager.saveOffset(topic, groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()


  }
}
