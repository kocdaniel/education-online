package com.atguigu.qz.controller

import com.atguigu.member.util.HiveUtil
import com.atguigu.qz.service.EtlDataService
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * 解析做题数据导入dwd层
  */
object DwdController {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_qz_controller")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = sparkSession.sparkContext

    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://HA")
    ssc.hadoopConfiguration.set("dfs.nameservices", "HA")

    HiveUtil.openDynamicPartition(sparkSession) // 开启动态分区
    HiveUtil.openCompression(sparkSession)
    HiveUtil.useSnappyCompression(sparkSession)

    EtlDataService.etlQzChapter(ssc, sparkSession)
    EtlDataService.etlQzChapterList(ssc, sparkSession)
    EtlDataService.etlQzPoint(ssc, sparkSession)
    EtlDataService.etlQzPointQuestion(ssc, sparkSession)
    EtlDataService.etlQzSiteCourse(ssc, sparkSession)
    EtlDataService.etlQzCourse(ssc, sparkSession)
    EtlDataService.etlQzCourseEdusubject(ssc, sparkSession)
    EtlDataService.etlQzWebsite(ssc, sparkSession)
    EtlDataService.etlQzMajor(ssc, sparkSession)
    EtlDataService.etlQzBusiness(ssc, sparkSession)
    EtlDataService.etlQzPaperView(ssc, sparkSession)
    EtlDataService.etlQzCenterPaper(ssc, sparkSession)
    EtlDataService.etlQzPaper(ssc, sparkSession)
    EtlDataService.etlQzCenter(ssc, sparkSession)
    EtlDataService.etlQzQuestion(ssc, sparkSession)
    EtlDataService.etlQzQuestionType(ssc, sparkSession)
    EtlDataService.etlQzMemberPaperQuestion(ssc, sparkSession)

  }
}
