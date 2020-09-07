package com.atguigu.gmall.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.util.{MyEsUtil, MyKafkaUtil, OffsetManager}
import com.atguigu.gmall.bean.{DauInfo, ProvinceInfo}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD

/**
 * 存储到Hbase中方便事实表进行查询
 */
object DimProvinceInfoApp {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dim_province_info_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_MM_BASE_PROVINCE"
    val groupId = "dim_province_info_group"
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
      val provinceRDD: RDD[ProvinceInfo] = rdd.map { jsonObj => //ex
        val provinceInfo: ProvinceInfo = ProvinceInfo(jsonObj.getString("id"),
          jsonObj.getString("name"),
          jsonObj.getString("area_code"),
          jsonObj.getString("iso_code"),
          jsonObj.getString("iso_3166_2"))
        println(provinceInfo)

        provinceInfo
      }
      //保存到HBase
      provinceRDD.saveToPhoenix("GMALL_PROVINCE_INFO",
        Seq("ID", "NAME", "AREA_CODE", "ISO_CODE", "ISO_3166_2"), new Configuration,
        Some("hadoop202,hadoop203,hadoop203:2181")
      )

      //提交偏移量
      OffsetManager.saveOffset(topic, groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()


  }

}
