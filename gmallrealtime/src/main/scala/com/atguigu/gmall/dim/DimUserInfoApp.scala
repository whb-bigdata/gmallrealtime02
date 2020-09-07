package com.atguigu.gmall.dim

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.util.{MyEsUtil, MyKafkaUtil, OffsetManager}
import com.atguigu.gmall.bean.UserInfo
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD


object DimUserInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dim_user_info_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_MM_USER_INFO"
    val groupId = "dim_user_info_group"
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

    //将数据转换为
    val jsonObjDstream: DStream[JSONObject] = inputWithOffsetDstream.map { record =>
      val jsonString: String = record.value()
      JSON.parseObject(jsonString)
    }


    //写入hbase (phoenix)
    jsonObjDstream.foreachRDD { rdd =>
      // xxxx driver
      val provinceRDD: RDD[UserInfo] = rdd.map { jsonObj => //ex
        //判断年龄段
        val birthday = jsonObj.getString("birthday")
        val year = birthday.split("-")(0)
        // println(year)
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
        val dateTimeStr: String = dateFormat.format(new Date())
        val nowyear = dateTimeStr.split("-")(0)
        // println(nowyear)
        val group = nowyear.toInt - year.toInt
        // println(group)
        var age_group = "老年"
        class Contains(r: Range) {
          def unapply(i: Int): Boolean = r contains i
        }
        val yang = new Contains(0 to 18)
        val strong = new Contains(19 to 40)
        group match {
          case yang() => age_group = "少年"
          case strong() => age_group = "青年"
          case _ => age_group = "老年"
        }
        //判断性别
        val genderE = jsonObj.getString("gender")
        var gender = "男"
        if (genderE.equals("F")) {
          gender = "女"
          age_group = "永远18岁"
        }
        //获取kafka流中的字段,进行赋值,给HBase数据库的内容
        val userInfo = UserInfo(
          jsonObj.getString("id").toInt,
          jsonObj.getString("name"),
          jsonObj.getString("birthday"),
          gender,
          age_group
        )
        println(userInfo)
        userInfo
      }
      //   println(provinceRDD) 并没有让上面的map执行,为什么??
      provinceRDD.saveToPhoenix("GMALL_USER_INFO",
        Seq("ID", "NAME", "BIRTHDAY", "GENDER", "AGE_GROUP"), new Configuration,
        Some("hadoop202,hadoop203,hadoop203:2181")
      )
      //进行保存到ES

      //提交偏移量
      OffsetManager.saveOffset(topic, groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}