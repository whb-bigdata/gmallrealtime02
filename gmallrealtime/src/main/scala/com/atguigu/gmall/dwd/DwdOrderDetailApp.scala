package com.atguigu.gmall.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall.bean.{OrderDetail, OrderInfo}
import com.atguigu.gmall.util.{MyKafkaSender, MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DwdOrderDetailApp {

  def main(args: Array[String]): Unit = {

    // 加载流 //手动偏移量
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dwd_order_detail_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "dwd_order_detail_group"
    val topic = "ODS_MM_ORDER_DETAIL";


    //1   从redis中读取偏移量   （启动执行一次）
    val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)

    //2   把偏移量传递给kafka ，加载数据流（启动执行一次）
    var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMapForKafka != null && offsetMapForKafka.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMapForKafka, groupId)
    } else {
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }


    //3   从流中获得本批次的 偏移量结束点（每批次执行一次）
    var offsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform { rdd => //周期性在driver中执行
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    // 1 提取数据
    val orderDetailDstream: DStream[OrderDetail] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
      val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
      orderDetail
    }

    //orderDetailDstream.print(1000)

    //////////////////////////
    /////////维度关联： spu id name , trademark id name , category3 id name/////////
    //////////////////////////

    // 合并维表数据
    // 品牌 分类 spu  作业
    //  orderDetailDstream.
    /////////////// 合并 商品信息////////////////////
    val orderInfoWithallDstream: DStream[OrderDetail] = orderDetailDstream.transform { rdd =>
      //todo 省份表方案二:将表中数据全部查询出来放到广播变量中,周期性获得广播变量中的内容,利用getorelse来获取,相应rdd省份编号的名称,并存放到orderInfo样例类中
      // driver中执行 周期性执行
      //todo 广播放skuinfo
      val skuJsonObjList: List[JSONObject] = PhoenixUtil.queryList("select * from GMALL_SKU_INFO  ")
      val skuInfoMap: Map[String, JSONObject] = skuJsonObjList.map(jsonObject => (jsonObject.getString("ID"), jsonObject)).toMap
      val skuInfoMapBC: Broadcast[Map[String, JSONObject]] = ssc.sparkContext.broadcast(skuInfoMap)

      //todo 广播放spuinfo
      val spuJsonObjList: List[JSONObject] = PhoenixUtil.queryList("select * from GMALL_SPU_INFO  ")
      val spuInfoMap: Map[String, JSONObject] = spuJsonObjList.map(jsonObject => (jsonObject.getString("ID"), jsonObject)).toMap
      val spuInfoMapBC: Broadcast[Map[String, JSONObject]] = ssc.sparkContext.broadcast(spuInfoMap)

      //todo 广播放base_trademark
      val trademarkJsonObjList: List[JSONObject] = PhoenixUtil.queryList("select * from GMALL_BASE_TRADEMARK")
      val trademarkMap: Map[String, JSONObject] = trademarkJsonObjList.map(jsonObject => (jsonObject.getString("TM_ID"), jsonObject)).toMap
      val trademarMapBC: Broadcast[Map[String, JSONObject]] = ssc.sparkContext.broadcast(trademarkMap)

      //todo 广播放category3
      val category3JsonObjList: List[JSONObject] = PhoenixUtil.queryList("select * from GMALL_BASE_CATEGORY3")
      val category3Map: Map[String, JSONObject] = category3JsonObjList.map(jsonObject => (jsonObject.getString("ID"), jsonObject)).toMap
      val category3MapBC: Broadcast[Map[String, JSONObject]] = ssc.sparkContext.broadcast(category3Map)

      val orderInfoWithallRdd: RDD[OrderDetail] = rdd.map { orderInfo =>
        //ex
        val skuMap: Map[String, JSONObject] = skuInfoMapBC.value
        //通过本条rdd获得广播变量中对应的那个kv,sku的kv
        val skuJsonObj: JSONObject = skuMap.getOrElse(orderInfo.sku_id.toString, null)

        //找到orderinfo中每个值对应sku表中的名称,并赋值给orderinfo,用来完善orderinfo表
        if (skuJsonObj != null) {
          orderInfo.spu_id = skuJsonObj.getString("SPUID").toLong
          orderInfo.tm_id = skuJsonObj.getString("TMID").toLong
          orderInfo.category3_id = skuJsonObj.getString("CATEGORY3ID").toLong
        }

        //todo 通过本条rdd获得广播变量中对应的那个kv,spu的kv.NAME
        val spuMap: Map[String, JSONObject] = spuInfoMapBC.value
        if(orderInfo.spu_id != null){
          val spuJsonObj: JSONObject = spuMap.getOrElse(orderInfo.spu_id.toString, null)
          if (spuJsonObj != null) {
            orderInfo.spu_name = spuJsonObj.getString("SPUNAME")
          }
        }


        //todo 通过本条rdd获得广播变量中对应的那个kv,trademar的kv.NAME
        val trademarMap: Map[String, JSONObject] = trademarMapBC.value
        if(orderInfo.tm_id!=null){       val trademarJsonObj: JSONObject = trademarMap.getOrElse(orderInfo.tm_id.toString, null)
          if (trademarJsonObj != null) {
            orderInfo.tm_name = trademarJsonObj.getString("TM_NAME")
          }}



        //todo 通过本条rdd获得广播变量中对应的那个kv,category3的kv.NAME
        val category3Map: Map[String, JSONObject] = category3MapBC.value
        if(orderInfo.category3_id !=null){      val category3JsonObj: JSONObject = category3Map.getOrElse(orderInfo.category3_id.toString, null)
          if (category3JsonObj != null) {
            orderInfo.category3_name = category3JsonObj.getString("NAME")
          }}


        //todo 节点进行操作,但是当这个节点没有数据进行操作的时候就不会进行处理也就不会进行打印
        println("ex进行处理")
        orderInfo
      }
      orderInfoWithallRdd
    }

    orderInfoWithallDstream.foreachRDD { rdd =>
      rdd.foreach { orderDetail =>
        MyKafkaSender.send("DWD_ORDER_DETAIL", JSON.toJSONString(orderDetail, new SerializeConfig(true)))
      }
      OffsetManager.saveOffset(topic, groupId, offsetRanges)
    }

    orderInfoWithallDstream.print(100)
    ssc.start()
    ssc.awaitTermination()
  }


}
