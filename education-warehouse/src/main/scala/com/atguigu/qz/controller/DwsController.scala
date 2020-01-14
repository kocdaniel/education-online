package com.atguigu.qz.controller

import com.atguigu.member.util.HiveUtil
import com.atguigu.qz.service.DwsQzService
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwsController {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dws_qz_controller")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext

    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://HA")
    ssc.hadoopConfiguration.set("dfs.nameservices", "HA")

    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩
    // HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩
    // 当参数需要从外部传入时
    // val dt = args(0)
    val dt = "20190722"
    DwsQzService.saveDwsQzChapter(sparkSession, dt)
    DwsQzService.saveDwsQzCourse(sparkSession, dt)
    DwsQzService.saveDwsQzMajor(sparkSession, dt)
    DwsQzService.saveDwsQzPaper(sparkSession, dt)
    DwsQzService.saveDwsQzQuestionTpe(sparkSession, dt)
    DwsQzService.saveDwsUserPaperDetail(sparkSession, dt)
  }
}
