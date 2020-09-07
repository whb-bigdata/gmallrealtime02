package com.atguigu.gmall

import com.atguigu.gmall.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object realtime {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val topic: String = "GMALL_START"
    val groupId: String = "dau_app_group"
    val input: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc,groupId)
    input.map(_.value()).print(100)
    ssc.start()
    ssc.awaitTermination()


  }

}
