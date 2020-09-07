package com.atguigu.gmall.util

import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.catalyst.expressions.GroupingID
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

object OffsetManager {


  def getOffset(topic: String, consumerGroupId: String): Map[TopicPartition,Long] = {
    val offsetKey = topic + ":" + consumerGroupId
    val jedis: Jedis = RedisUtil.getJedisClient
    val offsetJavaMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    jedis.close()
    import collection.JavaConverters._
    if (offsetJavaMap != null && offsetKey.size > 0) {
      val offsetList: List[(String, String)] = offsetJavaMap.asScala.toList
      val offsetListTp: List[(TopicPartition, Long)] = offsetList.map { case (p, s) =>
        println("分区:" + p + "偏移量:" + s)
        (new TopicPartition(topic, p.toInt), s.toLong)
      }
      offsetListTp.toMap
    } else {
      println("没有找着已存的偏移量")
      null
    }
  }


  def saveOffset(topic:String,groupID: String, offsetRangs: Array[OffsetRange])={
    val jedis: Jedis = RedisUtil.getJedisClient
    val offsetKey = topic + ":" + groupID
    val offset: util.HashMap[String, String] = new util.HashMap[String, String]()
    for (offsetrang <- offsetRangs){
      val partition: Int = offsetrang.partition
      val offset1: Long = offsetrang.untilOffset
      offset.put(partition.toString,offset1.toString)
      println("写入分区:" + partition + "写入偏移量:" + offset1)
    }
    jedis.hmset(offsetKey,offset)
    jedis.close()
  }
}
