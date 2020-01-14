package com.atguigu.qzpoint.streaming

import java.lang
import java.sql.{Connection, ResultSet}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.atguigu.qzpoint.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * 实时计算学员做题正确率与知识点掌握度
  */
object QzStreaming {

  private val groupid = "qz_group_test"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("RegisterStreaming").setMaster("local[*]")
      // 控制从kafka的消费速率，每个分区每秒100条
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      // 开启背压机制：消费速率根据下游的消费者的处理速率动态调整
      .set("spark.streaming.backpressure.enabled", "true")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // kafka 参数
    //kafka参数声明
    val topics = Array("qz_log")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop104:9092,hadoop104:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    // 查询mysql中是否有偏移量
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client: Connection = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()){
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            offsetMap.put(model, offset)
          }
          rs.close()
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }

    // 设置kafka消费数据的参数，判断本地是否有偏移量，有则根据偏移量继续消费，无则重新消费
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty){
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap)
      )
    }else{
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap)
      )
    }
    // 过滤脏数据
    val mapDStream: DStream[(String, String, String, String, String, String)] = inputDStream.filter(items => items.value().split("\t").length == 6)
      .mapPartitions(partition => {
        partition.map(items => {
          // 1002	504	20	0	1	2019-07-12 11:17:45
          // (用户id) (课程id) (知识点id) (题目id) (是否正确 0错误 1正确)(创建时间)
          val line: String = items.value()
          val arrs: Array[String] = line.split("\t")
          val uid = arrs(0)
          val courseid = arrs(1)
          val pointid = arrs(2)
          val questionid = arrs(3)
          val isright = arrs(4)
          val createtime = arrs(5)

          (uid, courseid, pointid, questionid, isright, createtime)
        })
      })

    mapDStream.foreachRDD(rdd=>{
      // 获取相同用户 同一课程 同一知识点的数据
      val groupRDD = rdd.groupBy(item=>item._1 + "-" + item._2 + "-" + item._3)
      groupRDD.foreachPartition(partition=>{
        // 在分区下获取jdbc链接
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try{
          partition.foreach{
            case (key, iters)=>{
              qzQuestionUpdate(key, iters, sqlProxy, client) // 对题库进行更新操作
            }
          }
        } catch {
          case e:Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }

      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 对题目表进行更新操作
    * @param key
    * @param iters
    * @param sqlProxy
    * @param client
    */
  def qzQuestionUpdate(key: String, iters: Iterable[(String, String, String, String, String, String)], sqlProxy: SqlProxy, client: Connection) = {
    val keys = key.split("-")
    val uid = keys(0).toInt
    val courseid = keys(1).toInt
    val pointid = keys(2).toInt

    val qzArray = iters.toArray
    val questionids: Array[String] = qzArray.map(_._4).distinct // 对当前批次下的questionid进行去重

    var questionids_history:Array[String] = Array()
    sqlProxy.executeQuery(client, "select questionids from qz_point_history where userid=? and courseid=? and pointid=?",
      Array(uid, courseid, pointid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()){
            // 将查询结果赋给历史题目表
            // 查询结果的下标是从1开始的，不是0
            questionids_history = rs.getString(1).split(",")
          }
          rs.close()
        }
      }
    )

    // 将获取到的历史数据与当前批次的数据进行拼接
    val resultQuestionids: Array[String] = questionids_history.union(questionids).distinct
    val countSize = resultQuestionids.length
    val resultQuestionids_str: String = resultQuestionids.mkString(",")
    // 当前批次去重后的做题个数
    val qzCount = questionids.length
    // 当前批次未去重时的做题总数
    var qzSum = qzArray.length
    // 获取当前批次做正确的题个数
    var qzIsRight = qzArray.filter(_._5.equals("1")).size
    // 获取当前批次最早的创建时间，作为表中的创建时间
    val createtime: String = qzArray.map(_._6).min

    // 更新qz_point_set记录表 此表用于存当前用户做过的questionid表
    val updateTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now())
    sqlProxy.executeUpdate(client, "insert into qz_point_history(userid,courseid,pointid,questionids,createtime,updatetime) values(?,?,?,?,?,?) " +
      " on duplicate key update questionids=?,updatetime=?", Array(uid, courseid, pointid, resultQuestionids_str, createtime, updateTime, resultQuestionids_str, updateTime))

    var qzSum_history = 0
    var isRight_history = 0

    sqlProxy.executeQuery(client, "select qz_sum,qz_istrue from qz_point_detail where userid=? and courseid=? and pointid=?",
      Array(uid, courseid, pointid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()){
            qzSum_history += rs.getInt(1)
            isRight_history += rs.getInt(2)
          }
          rs.close()
        }
      }
    )

    qzSum += qzSum_history
    qzIsRight += isRight_history

    // 计算正确率
    val correct_rate = qzIsRight.toDouble / qzSum.toDouble

    // 计算知识点掌握度：去重后的做题个数/当前知识点总题数（已知30题）*当前知识点的正确率
    // 假设每个知识点下有30个题目，先计算题的做题情况，再计算知识点掌握度
    val qz_detail_rate = countSize.toDouble / 30
    val mastery_rate = qz_detail_rate * correct_rate
    sqlProxy.executeUpdate(client, "insert into qz_point_detail(userid,courseid,pointid,qz_sum,qz_count,qz_istrue,correct_rate,mastery_rate,createtime,updatetime) values(?,?,?,?,?,?,?,?,?,?) " +
      "on duplicate key update qz_sum=?,qz_count=?,qz_istrue=?,correct_rate=?,mastery_rate=?,updatetime=?",
      Array(uid, courseid, pointid, qzSum, countSize, qzIsRight, correct_rate, mastery_rate, createtime, updateTime, qzSum, countSize, qzIsRight, correct_rate, mastery_rate, updateTime)
    )
  }

}
