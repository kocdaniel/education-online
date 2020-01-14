package com.atguigu.member.controller

import com.atguigu.member.service.EtlDataService
import com.atguigu.member.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwdMemberController {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dwd_member_import").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = sparkSession.sparkContext
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://HA")
    sc.hadoopConfiguration.set("dfs.nameservices", "HA")
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩
    // spark 默认使用snappy压缩
    //    HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩
    //对用户原始数据进行数据清洗 存入dwd层表中
    EtlDataService.etlBaseWebSiteLog(sc, sparkSession) //导入基础网站表数据
    EtlDataService.etlBaseAdLog(sc, sparkSession)
    EtlDataService.etlDwdMember(sc, sparkSession)
    EtlDataService.etlMemberRegtype(sc, sparkSession)
    EtlDataService.etlPcenterMemPayMoney(sc, sparkSession)
    EtlDataService.etlPcenterMemViplevel(sc, sparkSession)
  }
}
