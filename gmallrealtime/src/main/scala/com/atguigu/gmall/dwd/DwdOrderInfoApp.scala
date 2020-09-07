package com.atguigu.gmall.dwd

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.bean.OrderInfo
import com.atguigu.gmall.util.{MyEsUtil, MyKafkaSender, MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 存储到es中进行olap操作
 */
object DwdOrderInfoApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_order_info_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_MM_ORDER_INFO"
    val groupId = "dwd_order_info_group"
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
    //转换格式,变成样例类OrderInfo格式
    val orderInfoDstream: DStream[OrderInfo] = inputWithOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString,classOf[OrderInfo])
      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")

      orderInfo.create_date=createTimeArr(0)
      orderInfo.create_hour=createTimeArr(1).split(":")(0)

      orderInfo
    }
    //todo 省份表方案一:取出每条数据中的province_id,每条都利用PhoenixUtil的查询语句,获得该id所对应的名称
    /*
        orderInfoDstream.map { orderInfo =>
          val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList("select * from GMALL_PROVINCE_INFO where id='" + orderInfo.province_id + "'")
          val provinceJsonObj: JSONObject = provinceJsonObjList(0)
          orderInfo.province_3166_2_code = provinceJsonObj.getString("province_3166_2_code")
          //   orderInfo.province_id ==> orderInfo.province_3166_2_code
          orderInfo
        }
    */
    //todo 用户表方案:取出每条数据中的province_id,利用PhoenixUtil的查询语句,获得该id所对应的名称
    val orderInfoDstreamwithuser: DStream[OrderInfo] = orderInfoDstream.map { orderInfo =>
      val userJsonObjList: List[JSONObject] = PhoenixUtil.queryList("select * from GMALL_USER_INFO where id=" + orderInfo.user_id)
      if(userJsonObjList(0) != null){
      val userJsonObj: JSONObject = userJsonObjList(0)
      orderInfo.user_age_group = userJsonObj.getString("AGE_GROUP")
      orderInfo.user_gender = userJsonObj.getString("GENDER")}
      //   orderInfo.province_id ==> orderInfo.province_3166_2_code
      orderInfo
    }
    /*  todo 省份表方案一.二: 启动时执行 只执行一次,并利用广播变量存放查询的结果,这样可以每个周期查询一次广播变量,
         缺点:如果地区表改变,更新不及时
           val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList("select * from GMALL_PROVINCE_INFO  ")
           val provinceInfoMap: Map[String, JSONObject] = provinceJsonObjList.map(jsonObject=>(jsonObject.getString("id"),jsonObject)).toMap
           val provinceInfoMapBC: Broadcast[Map[String, JSONObject]] = ssc.sparkContext.broadcast(provinceInfoMap)
           orderInfoDstream.map { orderInfo  =>
             val provinceMap: Map[String, JSONObject] = provinceInfoMapBC.value
             val provinceJsonObj: JSONObject = provinceMap.getOrElse( orderInfo.province_id.toString,null )
             orderInfo.province_3166_2_code=provinceJsonObj.getString("province_3166_2_code")
             println(11111)
             orderInfo
           }
       println(222222222) // driver中执行 启动时执行 只执行一次*/
    val orderInfoWithProvinceuserDstream: DStream[OrderInfo] = orderInfoDstreamwithuser.transform { rdd =>
      //todo 省份表方案二:将表中数据全部查询出来放到广播变量中,周期性获得广播变量中的内容,利用getorelse来获取,相应rdd省份编号的名称,并存放到orderInfo样例类中
      // driver中执行 周期性执行
      val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList("select * from GMALL_PROVINCE_INFO  ")
      val provinceInfoMap: Map[String, JSONObject] = provinceJsonObjList.map(jsonObject => (jsonObject.getString("ID"), jsonObject)).toMap
      val provinceInfoMapBC: Broadcast[Map[String, JSONObject]] = ssc.sparkContext.broadcast(provinceInfoMap)
      val orderInfoWithProvinceRdd: RDD[OrderInfo] = rdd.map { orderInfo =>
        //ex
        val provinceMap: Map[String, JSONObject] = provinceInfoMapBC.value
        //通过本条rdd获得广播变量中对应的那个kv
        val provinceJsonObj: JSONObject = provinceMap.getOrElse(orderInfo.province_id.toString, null)
        //找到orderinfo中每个值对应provice表中的名称,并赋值给orderinfo,用来完善orderinfo表
        if (provinceJsonObj != null) {
          orderInfo.province_3166_2_code = provinceJsonObj.getString("ISO_3166_2")
          orderInfo.province_name = provinceJsonObj.getString("NAME")
          orderInfo.province_area_code = provinceJsonObj.getString("AREA_CODE")
        }
        //todo 节点进行操作,但是当这个节点没有数据进行操作的时候就不会进行处理也就不会进行打印
        println("ex进行处理")
        orderInfo
      }
      orderInfoWithProvinceRdd
    }

    orderInfoWithProvinceuserDstream.foreachRDD { rdd =>
      rdd.foreachPartition { orderInfoItr =>
        val orderInfolist: List[OrderInfo] = orderInfoItr.toList
        if (orderInfolist != null && orderInfolist.size > 0) {
          val create_date: String = orderInfolist(0).create_date

          val orderInfoWithIdList: List[(OrderInfo, String)] = orderInfolist.map(orderInfo => (orderInfo, orderInfo.id.toString))

          val indexName = "gmall_order_info_" + create_date
          MyEsUtil.saveDocBulk(orderInfoWithIdList, indexName)
        }
        for (orderInfo <- orderInfolist) {
          //todo 同时把这一批数据又发往kafka,这样ods层到dwd层的数据才算处理完成
          //todo fastjson不能直接把case class 转为jsonstring,需要,new SerializeConfig(true)这样处理才可以
          MyKafkaSender.send("DWD_ORDER_INFO", JSON.toJSONString(orderInfo, new SerializeConfig(true)))
        }
      }

      OffsetManager.saveOffset(topic, groupId, offsetRanges)
    }


    orderInfoWithProvinceuserDstream.print(100)


    ssc.start()
    ssc.awaitTermination()


  }

}
