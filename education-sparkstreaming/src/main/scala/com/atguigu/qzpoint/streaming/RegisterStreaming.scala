package com.atguigu.qzpoint.streaming

import java.lang

import com.atguigu.qzpoint.util.{DataSourceUtil, SqlProxy}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * 实时统计注册人员信息:
  * 需求1：实时统计注册人数，批次为3秒一批，使用updateStateBykey算子计算历史数据和当前批次的数据总数，仅此需求使用updateStateBykey，后续需求不使用updateStateBykey。
  * 需求2：每6秒统统计一次1分钟内的注册数据，不需要历史数据 提示:reduceByKeyAndWindow算子
  * 需求3：观察对接数据，尝试进行调优。
  */
object RegisterStreaming {
  private val groupid = "register_group_test"
  def main(args: Array[String]): Unit = {
    // KafkaConsumer is not safe for multi-threaded access ：解决方法：改成local[1]
    val sparkConf: SparkConf = new SparkConf().setAppName("RegisterStreaming").setMaster("local[*]")
      // 控制从kafka的消费速率，每个分区每秒100条
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      // .set("spark.streaming.kafka.consumer.cache.enabled", "false")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // kafka 参数
    //kafka参数声明
    val topics = Array("register_topic")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop104:9092,hadoop104:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    // ssc.checkpoint("/user/atguigu/sparkstreaming/checkpoint")

    val offsetMap = new mutable.HashMap[TopicPartition, Long]()

    // 设置kafka消费数据的参数，判断本地是否有偏移量，有则根据偏移量继续消费，无则重新消费
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty){
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }

    // Kafka ConsumerRecord is not serializable. Use .map to extract fields before calling .persist or .window
    // inputDStream.cache()
    // 过滤脏数据
    val resultDStream: DStream[(String, Int)] = inputDStream
      .filter(_.value().split("\t").length == 3)
      .mapPartitions(partition => {
        partition.map(item => {
          val line: String = item.value()
          val arrs: Array[String] = line.split("\t")
          val app_name: String = arrs(1) match {
            case "1" => "PC"
            case "2" => "APP"
            case _ => "Other"
          }
          (app_name, 1)
        })
      })
    resultDStream.cache()



    // TODO 需求一：实时统计注册人数，批次为3秒一批，使用updateStateBykey算子计算历史数据和当前批次的数据总数
    // 状态更新函数：
    // - 第一个参数：当前key对应的value
    // - 第二个参数：上一个阶段的value
/*
      val updateFunc = (values: Seq[Int], state: Option[Int])=>{
      val currentCount = values.sum // 本批次求和
      val previousCount = state.getOrElse(0) // 历史数据
      Some(currentCount + previousCount)
    }

    // 有状态的计算
    resultDStream.updateStateByKey(updateFunc).print()
*/

    // TODO 需求二：每6秒统统计一次1分钟内的注册数据，不需要历史数据
    resultDStream.reduceByKeyAndWindow((x:Int, y:Int)=>x+y, Seconds(60), Seconds(6)).print()


    // 处理完业务逻辑后， 手动提交offset维护到本地mysql中
//    inputDStream.foreachRDD(rdd=>{
//      val sqlProxy = new SqlProxy()
//      val client = DataSourceUtil.getConnection
//
//      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//
//      try {
//        for (or <- offsetRanges) {
//          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
//            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
//        }
//      } catch {
//        case e: Exception => e.printStackTrace()
//      } finally {
//        sqlProxy.shutdown(client)
//      }
//
//    })

    ssc.start()
    ssc.awaitTermination()
  }
}
